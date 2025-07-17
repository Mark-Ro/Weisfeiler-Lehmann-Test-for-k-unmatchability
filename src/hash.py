import xxhash

def fast_hash(s):
    return xxhash.xxh3_64(s).hexdigest()