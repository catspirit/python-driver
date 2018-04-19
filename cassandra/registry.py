# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class MessageCodecRegistry(object):
    encoders = None
    decoders = None

    def __init__(self):
        self.encoders = {}
        self.decoders = {}

    def _add(self, registry, protocol_version, opcode, func):
        if protocol_version not in registry:
            registry[protocol_version] = {}
        registry[protocol_version][opcode] = func

    def _get(self, registry, protocol_version, opcode):
        try:
            return registry[protocol_version][opcode]
        except KeyError:
            raise ValueError(
                "No codec registered for message '{0:02X}' and "
                "protocol version '{1}'".format(opcode, protocol_version))

    def add_encoder(self, protocol_version, opcode, encoder):
        return self._add(self.encoders, protocol_version, opcode, encoder)

    def add_decoder(self, protocol_version, opcode, decoder):
        return self._add(self.decoders, protocol_version, opcode, decoder)

    def get_encoder(self, protocol_version, opcode):
        return self._get(self.encoders, protocol_version, opcode)

    def get_decoder(self, protocol_version, opcode):
        return self._get(self.decoders, protocol_version, opcode)
