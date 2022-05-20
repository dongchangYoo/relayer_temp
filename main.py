import argparse

import toml
from chainpy.autotask.autotask import AutoTask
from chainpy.eth.managers.multichainmanager import EntityRootConfig
from event_chain import ChainEvent


parser = argparse.ArgumentParser(description="Run relayer")

parser.add_argument("--secretKey", "-k", type=str, help="enter admin key")
parser.add_argument("--relayerIndex", "-i", type=int, help="enter relayer index")

DEFAULT_RELAYER_KEY = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
RELAYER_CONFIG_PATH = "config/conf.relayer.toml"


def main(relayer_index: int, secret_key_: str):
    with open(RELAYER_CONFIG_PATH, "r") as f:
        config_dict = toml.load(f)
        entity_config: EntityRootConfig = EntityRootConfig.from_dict(config_dict)
        entity_config.account.secret_key = secret_key_
        entity_config.entity.index = relayer_index
        config_dict = entity_config.to_dict()

    relayer = AutoTask.from_dict(config_dict)
    relayer.register_event_type("Socket", ChainEvent)
    relayer.relayer_main()


if __name__ == "__main__":
    args = parser.parse_args()

    index = args.relayerIndex if args.relayerIndex is not None else 0
    secret_key = DEFAULT_RELAYER_KEY
    if args.secretKey is not None:
        secret_key = args.secretKey if len(args.secretKey) != 1 else args.secretKey * 64
        print("[Relayer] {}-th Relayer initiated by shell".format(index))
    else:
        print("[Relayer] {}-th Relayer initiated as default".format(index))
    main(index, secret_key)
