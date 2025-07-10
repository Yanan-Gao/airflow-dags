import warnings
from argparse import Namespace, ArgumentParser, ArgumentDefaultsHelpFormatter
from airflow.models.connection import Connection

warnings.simplefilter(action='ignore')


def setup_argument_parser() -> Namespace:
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)

    parser.add_argument('--conn-type', type=str, help='The connection type')
    parser.add_argument('--host', type=str, help='The host')
    parser.add_argument('--login', type=str, help='The login')
    parser.add_argument('--password', type=str, help='The password')
    parser.add_argument('--schema', type=str, help='The schema')
    parser.add_argument('--port', type=int, help='The port number')
    parser.add_argument(
        '--extra', type=str, help='Extra metadata. Non-standard data such as private/SSH keys can be saved here. '
        'JSON encoded object.'
    )

    args_namespace = parser.parse_args()

    distinct_args = set(vars(args_namespace).values())
    distinct_args.discard(None)

    if len(distinct_args) == 0:
        parser.error("At least one connection parameter should be specified")
    return args_namespace


def create_connection(args: Namespace) -> Connection:
    return Connection(
        conn_type=args.conn_type,
        host=args.host,
        login=args.login,
        password=args.password,
        schema=args.schema,
        port=args.port,
        extra=args.extra
    )


if __name__ == '__main__':
    args = setup_argument_parser()
    connection = create_connection(args)

    print(connection.get_uri())
