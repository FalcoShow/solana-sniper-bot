import base58
import json

# Replace this with your base58 private key (string of letters/numbers)
base58_key = "4dWeVftX2TGrvEsSPboQ5yeyQqrsPKZGs53w5E5fb9ooXoLDVBqdQa7JUTVeuSxV3TeL4bNcaqwdgBYwXL4kFzEn"

# Convert to bytes, then list of ints
decoded = base58.b58decode(base58_key)
key_list = list(decoded)

# Print JSON array
print(json.dumps(key_list))
