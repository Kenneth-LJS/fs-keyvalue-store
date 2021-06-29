import time

from keyvaluestore import StoreManager
# from keyvaluestore.keyhasher import KeyHasher
from lib import *

def main():

    json_dump_args = {
        'indent': 4,
    }

    # # Compact json
    # json_dump_args = {
    #     'separators': (',', ':'),
    # }

    store_manager_args = {
        'fs_path': '_data',
        'key_hash_func': hash_word_key,
        'key_hash_len': 32,
        'serialize_key': serialize_word_key,
        'deserialize_key': deserialize_word_key,
        'serialize_value': serialize_word_value,
        'deserialize_value': deserialize_word_value,
        'json_dump_args': json_dump_args,
    }

    # key_hasher = KeyHasher(
    #     hash_func=hash_word_key,
    #     hash_size=32,
    # )

    with StoreManager[WordKey, WordValue](**store_manager_args) as store:
        for word_key in get_word_keys():
            print(word_key)
            # if word_key in store:
            #     pass
                # print('Skipping', word_key)
                # continue



            # print(word_key, key_hasher.hash(word_key).hex_list)

            # word_value = compute_word_value(word_key)
            # time.sleep(0.5)
            # print('word_value', word_value)
            # store[word_key] = word_value

        print('Done')

if __name__ == '__main__':
    main()