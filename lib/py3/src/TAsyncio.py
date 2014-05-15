#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from io import BytesIO
import logging
import struct
import asyncio

from thrift.transport import TTransport


class TAsyncioTransport(TTransport.TTransportBase):
    """a framed, buffered transport over a Tornado stream"""
    def __init__(self, reader, writer):
        self.__reader = reader
        self.__writer = writer
        self.__wbuf = BytesIO()

    # not the same number of parameters as TTransportBase.open
    @classmethod
    @asyncio.coroutine
    def connect(TAsyncioTransport, host, port):
        reader, writer = yield from asyncio.open_connection(host, port)
        return TAsyncioTransport(reader, writer)

    def read(self, _):
        # The generated code for Tornado shouldn't do individual reads -- only
        # frames at a time
        assert "you're doing it wrong" is True

    @asyncio.coroutine
    def readFrame(self):
        frame_header = yield from self.__reader.readexactly(4)
        frame_length, = struct.unpack('!i', frame_header)
        frame = yield from self.__reader.readexactly(frame_length)
        return frame

    def write(self, buf):
        self.__wbuf.write(buf)

    @asyncio.coroutine
    def flush(self, callback=None):
        wout = self.__wbuf.getvalue()
        wsz = len(wout)
        # reset wbuf before write/flush to preserve state on underlying failure
        self.__wbuf.truncate(0)
        self.__wbuf.seek(0)

        # WARNING: ensure no yield in-between
        self.__writer.write(struct.pack("!i", wsz) + wout)

        logging.debug('writing frame length = %i', wsz)

        # The drain here is mostly useful for one-way methods
        yield from self.__writer.drain()

    def close(self):
        self.__reader.close()
        self.__writer.close()
