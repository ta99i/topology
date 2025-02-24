from binascii import hexlify


import io
import struct
import ipaddress


class ChannelAnnouncement(object):
    def __init__(self):
        self.num_short_channel_id = None
        self.node_signatures = [None, None]
        self.bitcoin_signatures = [None, None]
        self.features = None
        self.chain_hash = None
        self.node_ids = [None, None]
        self.bitcoin_keys = [None, None]

    @property
    def short_channel_id(self):
        return "{}x{}x{}".format(
            (self.num_short_channel_id >> 40) & 0xFFFFFF,
            (self.num_short_channel_id >> 16) & 0xFFFFFF,
            (self.num_short_channel_id >> 00) & 0xFF,
        )

    def __eq__(self, other):
        return (
            self.num_short_channel_id == other.num_short_channel_id
            and self.bitcoin_keys == other.bitcoin_keys
            and self.chain_hash == other.chain_hash
            and self.node_ids == other.node_ids
            and self.features == other.features
        )

    def serialize(self):
        raise ValueError()

    def __str__(self):
        na = hexlify(self.node_ids[0]).decode("ASCII")
        nb = hexlify(self.node_ids[1]).decode("ASCII")
        return "ChannelAnnouncement(scid={short_channel_id}, nodes=[{na},{nb}])".format(
            na=na, nb=nb, short_channel_id=self.short_channel_id
        )

    def __json__(self):
        return {
            '_type': 'channel_announcement',
            'scid': self.short_channel_id,
            'features': hexlify(self.features).decode('ASCII'),
            'node_id_1': hexlify(self.node_ids[0]).decode('ASCII'),
            'node_id_2': hexlify(self.node_ids[1]).decode('ASCII'),
            'chain_hash': hexlify(self.chain_hash).decode('ASCII'),
        }


class ChannelUpdate(object):
    def __init__(self):
        self.signature = None
        self.chain_hash = None
        self.num_short_channel_id = None
        self.timestamp = None
        self.message_flags = None
        self.channel_flags = None
        self.cltv_expiry_delta = None
        self.htlc_minimum_msat = None
        self.fee_base_msat = None
        self.fee_proportional_millionths = None
        self.htlc_maximum_msat = None

    def __json__(self):
        return {
            '_type': 'channel_update',
            'scid': self.short_channel_id,
            'timestamp': self.timestamp,
            'message_flags': hexlify(self.message_flags).decode('ASCII'),
            'channel_flags': hexlify(self.channel_flags).decode('ASCII'),
            'cltv_expiry_delta': self.cltv_expiry_delta,
            'htlc_minimum_msat': self.htlc_minimum_msat,
            'fee_base_msat': self.fee_base_msat,
            'fee_proportional_millionths': self.fee_proportional_millionths,
            'htlc_maximum_msat': self.htlc_maximum_msat,
            'disabled': bool(self.disable),
            'chain_hash': hexlify(self.chain_hash).decode('ASCII'),
        }

    @property
    def short_channel_id(self):
        return "{}x{}x{}".format(
            (self.num_short_channel_id >> 40) & 0xFFFFFF,
            (self.num_short_channel_id >> 16) & 0xFFFFFF,
            (self.num_short_channel_id >> 00) & 0xFF,
        )

    @property
    def direction(self):
        (b,) = struct.unpack("!B", self.channel_flags)
        return b & 0x01

    @property
    def disable(self):
        (b,) = struct.unpack("!B", self.channel_flags)
        return (b >> 1) & 0x01

    def serialize(self):
        raise ValueError()

    def __str__(self):
        return "ChannelUpdate(scid={short_channel_id}, timestamp={timestamp})".format(
            timestamp=self.timestamp, short_channel_id=self.short_channel_id
        )

    def __eq__(self, other):
        return (
            self.chain_hash == other.chain_hash
            and self.num_short_channel_id == other.num_short_channel_id
            and self.timestamp == other.timestamp
            and self.message_flags == other.message_flags
            and self.channel_flags == other.channel_flags
            and self.cltv_expiry_delta == other.cltv_expiry_delta
            and self.htlc_minimum_msat == other.htlc_minimum_msat
            and self.fee_base_msat == other.fee_base_msat
            and self.fee_proportional_millionths == other.fee_proportional_millionths
            and self.htlc_maximum_msat == other.htlc_maximum_msat
        )


class Address(object):
    def __init__(self, typ=None, addr=None, port=None):
        self.typ = typ
        self.addr = addr
        self.port = port

    def __eq__(self, other):
        return (
            self.typ == other.typ
            and self.addr == other.addr
            and self.port == other.port
        )

    def __len__(self):
        l = {
            1: 6,
            2: 18,
            3: 12,
            4: 37,
        }
        return l[self.typ] + 1

    def __str__(self):
        addr = self.addr
        if self.typ == 1:
            addr = ".".join([str(c) for c in addr])

        protos = {
            1: "ipv4",
            2: "ipv6",
            3: "torv2",
            4: "torv3",
        }

        proto = protos.get(self.typ, 'UNKNOWN')
        return f"{proto}://{addr}:{self.port}"


class NodeAnnouncement(object):
    def __init__(self):
        self.signature = None
        self.features = ""
        self.timestamp = None
        self.node_id = None
        self.rgb_color = None
        self.alias = None
        self.addresses = None

    def __str__(self):
        return "NodeAnnouncement(id={hexlify(node_id)}, alias={alias}, color={rgb_color})".format(
            node_id=self.node_id, alias=self.alias, rgb_color=self.rgb_color
        )

    def __eq__(self, other):
        return (
            self.features == other.features
            and self.timestamp == other.timestamp
            and self.node_id == other.node_id
            and self.rgb_color == other.rgb_color
            and self.alias == other.alias
        )

    def __json__(self):
        return {
            '_type': 'node_announcement',
            'features': self.features.hex(),
            'timestamp': self.timestamp,
            'node_id': self.node_id.hex(),
            'rgb_color': self.rgb_color.hex(),
            'addresses': []  # TODO Add missing addresses
        }


def parse(b):
    if not isinstance(b, io.BytesIO):
        b = io.BytesIO(b)
    (typ,) = struct.unpack("!H", b.read(2))

    parsers = {
        256: parse_channel_announcement,
        257: parse_node_announcement,
        258: parse_channel_update,
        3503: parse_ignore,
    }

    if typ not in parsers:
        raise ValueError("No parser registered for type {typ}".format(typ=typ))

    return parsers[typ](b)


def parse_ignore(b):
    return None


def parse_channel_announcement(b):
    if not isinstance(b, io.BytesIO):
        b = io.BytesIO(b)

    ca = ChannelAnnouncement()
    ca.node_signatures = (b.read(64), b.read(64))
    ca.bitcoin_signatures = (b.read(64), b.read(64))
    (flen,) = struct.unpack("!H", b.read(2))
    ca.features = b.read(flen)
    ca.chain_hash = b.read(32)[::-1]
    (ca.num_short_channel_id,) = struct.unpack("!Q", b.read(8))
    ca.node_ids = (b.read(33), b.read(33))
    ca.bitcoin_keys = (b.read(33), b.read(33))
    return ca


def parse_channel_update(b):
    if not isinstance(b, io.BytesIO):
        b = io.BytesIO(b)

    cu = ChannelUpdate()
    cu.signature = b.read(64)
    cu.chain_hash = b.read(32)[::-1]
    (cu.num_short_channel_id,) = struct.unpack("!Q", b.read(8))
    (cu.timestamp,) = struct.unpack("!I", b.read(4))
    cu.message_flags = b.read(1)
    cu.channel_flags = b.read(1)
    (cu.cltv_expiry_delta,) = struct.unpack("!H", b.read(2))
    (cu.htlc_minimum_msat,) = struct.unpack("!Q", b.read(8))
    (cu.fee_base_msat,) = struct.unpack("!I", b.read(4))
    (cu.fee_proportional_millionths,) = struct.unpack("!I", b.read(4))
    t = b.read(8)
    if len(t) == 8:
        (cu.htlc_maximum_msat,) = struct.unpack("!Q", t)
    else:
        cu.htlc_maximum_msat = None

    return cu


def parse_address(b):
    if not isinstance(b, io.BytesIO):
        b = io.BytesIO(b)

    t = b.read(1)
    if len(t) != 1:
        return None

    a = Address()
    (a.typ,) = struct.unpack("!B", t)

    if a.typ == 1:
        a.addr = b.read(4)
        (a.port,) = struct.unpack("!H", b.read(2))
    elif a.typ == 2:
        a.addr = b.read(16)
        a.addr = '[' + format(ipaddress.IPv6Address(a.addr)) + ']'
        (a.port,) = struct.unpack("!H", b.read(2))
    elif a.typ == 3:
        a.addr = b.read(10)
        a.addr = to_base_32(a.addr) + '.onion'
        (a.port,) = struct.unpack("!H", b.read(2))
    elif a.typ == 4:
        a.addr = b.read(35)
        a.addr = to_base_32(a.addr) + '.onion'
        (a.port,) = struct.unpack("!H", b.read(2))
    else:
        a.addr = b.getvalue()[1:]
        a.port = None
    return a

# https://github.com/alexbosworth/bolt07/blob/519c94a7837e687bf7478a74779d5ea493a76a44/addresses/encode_base32.js


def to_base_32(addr):
    alphabet = 'abcdefghijklmnopqrstuvwxyz234567'
    byte = 8
    lastIndex = 31
    word = 5
    bits = 0
    base32 = ''
    value = 0

    for char in addr:
        bits += byte
        value = (value << byte) | char

        while bits >= word:
            base32 += alphabet[(value >> (bits - word)) & lastIndex]
            bits -= word

    if bits > 0:
        base32 += alphabet[(value << (word - bits)) & lastIndex]

    return base32


def parse_node_announcement(b):
    if not isinstance(b, io.BytesIO):
        b = io.BytesIO(b)

    na = NodeAnnouncement()
    na.signature = b.read(64)
    (flen,) = struct.unpack("!H", b.read(2))
    na.features = b.read(flen)
    (na.timestamp,) = struct.unpack("!I", b.read(4))
    na.node_id = b.read(33)
    na.rgb_color = b.read(3)
    na.alias = b.read(32)
    (alen,) = struct.unpack("!H", b.read(2))
    abytes = io.BytesIO(b.read(alen))
    na.addresses = []
    while True:
        addr = parse_address(abytes)
        if addr is None:
            break
        else:
            na.addresses.append(addr)

    return na
