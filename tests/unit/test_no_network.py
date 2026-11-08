"""Guards the rule that no test reaches the internet.

pytest-socket is configured in pyproject.toml to allow localhost only.
"""

from __future__ import annotations

import socket

import pytest
from pytest_socket import SocketConnectBlockedError


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_connecting_to_a_public_address_is_blocked() -> None:
    with (
        pytest.raises(SocketConnectBlockedError),
        socket.create_connection(("93.184.215.14", 443), timeout=1),
    ):
        pass
