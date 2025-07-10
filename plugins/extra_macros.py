from typing import Optional

from airflow.plugins_manager import AirflowPlugin


def resolve_consul_url(dns_name: str, port: Optional[int] = None, limit: int = 10) -> str:
    import dns.resolver
    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = 100
        resolver.lifetime = 100
        result = resolver.resolve(dns_name, rdtype='A')  # 'A' record for IPv4 addresses
        if port:
            ips = [f"{ip.address}:{port}" for ip in result]  # type: ignore
        else:
            ips = [f"{ip.address}" for ip in result]  # type: ignore
        top_10_ips = ips[:limit]
        aerospike_host = ",".join(top_10_ips)
        print(aerospike_host)
        return aerospike_host
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout) as e:
        print(f"Failed to resolve IPs: {e}")
        return ""


class TtdExtrasPlugin(AirflowPlugin):
    name = "ttd_extras"
    macros = [resolve_consul_url]
