# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2010 OpenStack LLC.
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Implementation of an S3-like storage server based on local files.

Useful to test features that will eventually run on S3, or if you want to
run something locally that was once running on S3.

We don't support all the features of S3, but it does work with the
standard S3 client for the most basic semantics. To use the standard
S3 client with this module::

    c = S3.AWSAuthConnection("", "", server="localhost", port=8888,
                             is_secure=False)
    c.create_bucket("mybucket")
    c.put("mybucket", "mykey", "a value")
    print c.get("mybucket", "mykey").body

"""

import bisect
import datetime
import hashlib
import os
import os.path
import urllib

import routes
import webob

from nova import flags
from nova.openstack.common import cfg
from nova import utils
from nova import wsgi


s3_opts = [
    cfg.StrOpt('buckets_path',
               default='$state_path/buckets',
               help='path to s3 buckets'),
    cfg.StrOpt('s3_listen',
               default="0.0.0.0",
               help='IP address for S3 API to listen'),
    cfg.IntOpt('s3_listen_port',
               default=3333,
               help='port for s3 api to listen'),
]

FLAGS = flags.FLAGS
FLAGS.register_opts(s3_opts)


def get_wsgi_server():
    return wsgi.Server("S3 Objectstore",
                       S3Application(FLAGS.buckets_path),
                       port=FLAGS.s3_listen_port,
                       host=FLAGS.s3_listen)


class S3Application(wsgi.Router):
    """Implementation of an S3-like storage server based on local files.

    If bucket depth is given, we break files up into multiple directories
    to prevent hitting file system limits for number of files in each
    directories. 1 means one level of directories, 2 means 2, etc.

    """

    def __init__(self, root_directory, bucket_depth=0, mapper=None):
        if mapper is None:
            mapper = routes.Mapper()

        mapper.connect('/',
                controller=lambda *a, **kw: RootHandler(self)(*a, **kw))
        mapper.connect('/{bucket}/{object_name}',
                controller=lambda *a, **kw: ObjectHandler(self)(*a, **kw))
        mapper.connect('/{bucket_name}/',
                controller=lambda *a, **kw: BucketHandler(self)(*a, **kw))
        self.directory = os.path.abspath(root_directory)
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        self.bucket_depth = bucket_depth
        super(S3Application, self).__init__(mapper)


class BaseRequestHandler(object):
    """Base class emulating Tornado's web framework pattern in WSGI.

    This is a direct port of Tornado's implementation, so some key decisions
    about how the code interacts have already been chosen.

    The two most common ways of designing web frameworks can be
    classified as async object-oriented and sync functional.

    Tornado's is on the OO side because a response is built up in and using
    the shared state of an object and one of the object's methods will
    eventually trigger the "finishing" of the response asynchronously.

    Most WSGI stuff is in the functional side, we pass a request object to
    every call down a chain and the eventual return value will be a response.

    Part of the function of the routing code in S3Application as well as the
    code in BaseRequestHandler's __call__ method is to merge those two styles
    together enough that the Tornado code can work without extensive
    modifications.

    To do that it needs to give the Tornado-style code clean objects that it
    can modify the state of for each request that is processed, so we use a
    very simple factory lambda to create new state for each request, that's
    the stuff in the router, and when we let the Tornado code modify that
    object to handle the request, then we return the response it generated.
    This wouldn't work the same if Tornado was being more async'y and doing
    other callbacks throughout the process, but since Tornado is being
    relatively simple here we can be satisfied that the response will be
    complete by the end of the get/post method.

    """

    def __init__(self, application):
        self.application = application

    @webob.dec.wsgify
    def __call__(self, request):
        method = request.method.lower()
        f = getattr(self, method, self.invalid)
        self.request = request
        self.response = webob.Response()
        params = request.environ['wsgiorg.routing_args'][1]
        del params['controller']
        f(**params)
        return self.response

    def get_argument(self, arg, default):
        return self.request.params.get(arg, default)

    def set_header(self, header, value):
        self.response.headers[header] = value

    def set_status(self, status_code):
        self.response.status = status_code

    def finish(self, body=''):
        self.response.body = utils.utf8(body)

    def invalid(self, **kwargs):
        pass

    def render_xml(self, value):
        assert isinstance(value, dict) and len(value) == 1
        self.set_header("Content-Type", "application/xml; charset=UTF-8")
        name = value.keys()[0]
        parts = []
        parts.append('<' + utils.utf8(name) +
                     ' xmlns="http://doc.s3.amazonaws.com/2006-03-01">')
        self._render_parts(value.values()[0], parts)
        parts.append('</' + utils.utf8(name) + '>')
        self.finish('<?xml version="1.0" encoding="UTF-8"?>\n' +
                    ''.join(parts))

    def _render_parts(self, value, parts=None):
        if not parts:
            parts = []

        if isinstance(value, basestring):
            parts.append(utils.xhtml_escape(value))
        elif isinstance(value, int) or isinstance(value, long):
            parts.append(str(value))
        elif isinstance(value, datetime.datetime):
            parts.append(value.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        elif isinstance(value, dict):
            for name, subvalue in value.iteritems():
                if not isinstance(subvalue, list):
                    subvalue = [subvalue]
                for subsubvalue in subvalue:
                    parts.append('<' + utils.utf8(name) + '>')
                    self._render_parts(subsubvalue, parts)
                    parts.append('</' + utils.utf8(name) + '>')
        else:
            raise Exception("Unknown S3 value type %r", value)

    def _object_path(self, bucket, object_name):
        if self.application.bucket_depth < 1:
            return os.path.abspath(os.path.join(
                self.application.directory, bucket, object_name))
        hash = hashlib.md5(object_name).hexdigest()
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket))
        for i in range(self.application.bucket_depth):
            path = os.path.join(path, hash[:2 * (i + 1)])
        return os.path.join(path, object_name)


class RootHandler(BaseRequestHandler):
    def get(self):
        names = os.listdir(self.application.directory)
        buckets = []
        for name in names:
            path = os.path.join(self.application.directory, name)
            info = os.stat(path)
            buckets.append({
                "Name": name,
                "CreationDate": datetime.datetime.utcfromtimestamp(
                    info.st_ctime),
            })
        self.render_xml({"ListAllMyBucketsResult": {
            "Buckets": {"Bucket": buckets},
        }})


class BucketHandler(BaseRequestHandler):
    def get(self, bucket_name):
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        max_keys = int(self.get_argument("max-keys", 50000))
        path = os.path.abspath(os.path.join(self.application.directory,
                                            bucket_name))
        terse = int(self.get_argument("terse", 0))
        if (not path.startswith(self.application.directory) or
            not os.path.isdir(path)):
            self.set_status(404)
            return
        object_names = []
        for root, dirs, files in os.walk(path):
            for file_name in files:
                object_names.append(os.path.join(root, file_name))
        skip = len(path) + 1
        for i in range(self.application.bucket_depth):
            skip += 2 * (i + 1) + 1
        object_names = [n[skip:] for n in object_names]
        object_names.sort()
        contents = []

        start_pos = 0
        if marker:
            start_pos = bisect.bisect_right(object_names, marker, start_pos)
        if prefix:
            start_pos = bisect.bisect_left(object_names, prefix, start_pos)

        truncated = False
        for object_name in object_names[start_pos:]:
            if not object_name.startswith(prefix):
                break
            if len(contents) >= max_keys:
                truncated = True
                break
            object_path = self._object_path(bucket_name, object_name)
            c = {"Key": object_name}
            if not terse:
                info = os.stat(object_path)
                c.update({
                    "LastModified": datetime.datetime.utcfromtimestamp(
                        info.st_mtime),
                    "Size": info.st_size,
                })
            contents.append(c)
            marker = object_name
        self.render_xml({"ListBucketResult": {
            "Name": bucket_name,
            "Prefix": prefix,
            "Marker": marker,
            "MaxKeys": max_keys,
            "IsTruncated": truncated,
            "Contents": contents,
        }})

    def put(self, bucket_name):
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket_name))
        if (not path.startswith(self.application.directory) or
            os.path.exists(path)):
            self.set_status(403)
            return
        os.makedirs(path)
        self.finish()

    def delete(self, bucket_name):
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket_name))
        if (not path.startswith(self.application.directory) or
            not os.path.isdir(path)):
            self.set_status(404)
            return
        if len(os.listdir(path)) > 0:
            self.set_status(403)
            return
        os.rmdir(path)
        self.set_status(204)
        self.finish()


class ObjectHandler(BaseRequestHandler):
    def get(self, bucket, object_name):
        object_name = urllib.unquote(object_name)
        path = self._object_path(bucket, object_name)
        if (not path.startswith(self.application.directory) or
            not os.path.isfile(path)):
            self.set_status(404)
            return
        info = os.stat(path)
        self.set_header("Content-Type", "application/unknown")
        self.set_header("Last-Modified", datetime.datetime.utcfromtimestamp(
            info.st_mtime))
        object_file = open(path, "r")
        try:
            self.finish(object_file.read())
        finally:
            object_file.close()

    def put(self, bucket, object_name):
        object_name = urllib.unquote(object_name)
        bucket_dir = os.path.abspath(os.path.join(
            self.application.directory, bucket))
        if (not bucket_dir.startswith(self.application.directory) or
            not os.path.isdir(bucket_dir)):
            self.set_status(404)
            return
        path = self._object_path(bucket, object_name)
        if not path.startswith(bucket_dir) or os.path.isdir(path):
            self.set_status(403)
            return
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        object_file = open(path, "w")
        object_file.write(self.request.body)
        object_file.close()
        self.set_header('ETag',
                        '"%s"' % hashlib.md5(self.request.body).hexdigest())
        self.finish()

    def delete(self, bucket, object_name):
        object_name = urllib.unquote(object_name)
        path = self._object_path(bucket, object_name)
        if (not path.startswith(self.application.directory) or
            not os.path.isfile(path)):
            self.set_status(404)
            return
        os.unlink(path)
        self.set_status(204)
        self.finish()
