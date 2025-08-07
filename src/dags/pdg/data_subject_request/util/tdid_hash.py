import hashlib
import uuid


def tdid_hash(id):
    """
    Converts an ID (EUID, UID2 etc) to TDIDs.  This is copied from AdPlatform IdentityHashing class
    https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/TTD.Domain.Bidding.Public/Identity/IdentityHashing.cs#L15-26
    """
    ascii_bytes = bytes(id, 'ascii')
    hasher = hashlib.md5(ascii_bytes)
    return str(uuid.UUID(bytes=hasher.digest()))
