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

from cassandra.protocol import *
from cassandra.registry import MessageCodecRegistry
from cassandra.protocol import ProtocolHandler

__all__ = ['DriverContext']


class DriverContext(object):

    _message_codec_registry = None
    _protocol_handler = None  # the default protocol handler

    @property
    def message_codec_registry(self):
        if not self._message_codec_registry:
            self._message_codec_registry = self._build_message_codec_registry()
        return self._message_codec_registry

    @property
    def protocol_handler(self):
        if not self._protocol_handler:
            self._protocol_handler = ProtocolHandler(
                self.message_codec_registry.encoders,
                self.message_codec_registry.decoders)
        return self._protocol_handler

    @staticmethod
    def _build_message_codec_registry():
        registry = MessageCodecRegistry()
        # TODO will be get from the DriverContext protocol version registry later
        protocol_versions = (ProtocolVersion.V3, ProtocolVersion.V4, ProtocolVersion.V5)
        for v in protocol_versions:
            for message in [
                StartupMessage,
                RegisterMessage,
                BatchMessage,
                QueryMessage,
                ExecuteMessage,
                PrepareMessage,
                OptionsMessage,
                AuthResponseMessage,
            ]:
                registry.add_encoder(v, message.opcode, message.encode)

            error_decoders = [(e.error_code, e.decode) for e in [
                UnavailableErrorMessage,
                ReadTimeoutErrorMessage,
                WriteTimeoutErrorMessage,
                IsBootstrappingErrorMessage,
                OverloadedErrorMessage,
                UnauthorizedErrorMessage,
                ServerError,
                ProtocolException,
                BadCredentials,
                TruncateError,
                ReadFailureMessage,
                FunctionFailureMessage,
                WriteFailureMessage,
                CDCWriteException,
                SyntaxException,
                InvalidRequestException,
                ConfigurationException,
                PreparedQueryNotFound,
                AlreadyExistsException
            ]]

            for codec in [
                ReadyMessage,
                EventMessage.Codec,
                ResultMessage.Codec,
                AuthenticateMessage,
                AuthSuccessMessage,
                AuthChallengeMessage,
                SupportedMessage,
                ErrorMessage.Codec(error_decoders)

            ]:
                registry.add_decoder(v, codec.opcode, codec.decode)

        return registry
