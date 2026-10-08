"""LDAP lookup and authentication. Never a file-transfer source or destination."""
import ssl
from contextlib import contextmanager
from urllib.parse import urlsplit

import ldap3
from ldap3.utils.conv import escape_filter_chars
from .protocols import ConnectorError, secret


def servers(connection,options):
    parsed=urlsplit(connection["endpoint"])
    hosts=[(parsed.hostname,parsed.port or (636 if parsed.scheme=="ldaps" else 389))]
    if options.srv_records:
        hosts=[]
        for record in options.srv_records:
            name,separator,port=record.rpartition(":")
            if not separator or not name or not port.isdigit() or not 1<=int(port)<=65535:
                raise ConnectorError("LDAP SRV records must be hostname:port")
            hosts.append((name,int(port)))
    if options.automatic_dns_lookup:
        import dns.resolver
        query=("_ldaps._tcp." if parsed.scheme=="ldaps" else "_ldap._tcp.")+options.dns_domain
        records=dns.resolver.resolve(query,"SRV",lifetime=options.timeout)
        hosts=[(str(row.target).rstrip("."),row.port) for row in sorted(records,key=lambda row:row.priority)]
    return parsed.scheme,hosts


@contextmanager
def session(connection,options,user=None,password=None):
    scheme,hosts=servers(connection,options)
    tls=ldap3.Tls(validate=ssl.CERT_REQUIRED,ca_certs_data=secret(options.ca_certificate_env) if options.ca_certificate_env else None)
    bound=None
    for host,port in hosts:
        server=ldap3.Server(host,port=port,use_ssl=scheme=="ldaps",tls=tls,connect_timeout=options.timeout,get_info=ldap3.NONE)
        candidate=ldap3.Connection(server,user=user if user is not None else options.bind_dn,
            password=password if password is not None else secret(options.password_env,True),
            receive_timeout=options.timeout,auto_referrals=False,raise_exceptions=False)
        try:
            candidate.open()
            if scheme!="ldaps" and options.start_tls and not candidate.start_tls():
                raise ConnectorError("LDAP StartTLS negotiation failed")
            if not candidate.bind():
                candidate.unbind()
                continue
            bound=candidate
            break
        except Exception:
            candidate.unbind()
    if bound is None:
        raise ConnectorError("LDAP bind failed; verify server, TLS, credentials and permissions")
    try:
        yield bound
    finally:
        bound.unbind()


def test_directory(connection,options):
    with session(connection,options):
        return {"reachable":True,"role":"authentication","detail":"Directory bind succeeded"}


def lookup(connection,options,username=None):
    if not options.base_dn:
        raise ConnectorError("LDAP Base DN is required")
    filter_=options.search_filter
    if username is not None:
        filter_=f"(&{filter_}({options.username_attribute}={escape_filter_chars(username)}))"
    fields=[options.username_attribute,options.email_attribute,options.uid_attribute,options.first_name_attribute,
            options.last_name_attribute,options.full_name_attribute,options.phone_attribute,options.home_directory_attribute]
    fields=list(dict.fromkeys(field for field in fields if field))
    with session(connection,options) as client:
        found=client.search(options.base_dn,filter_,attributes=fields,
            size_limit=2 if username is not None else options.max_entries+1,time_limit=options.timeout)
        result_code=client.result.get("result",0)
        if result_code not in (0,4):
            raise ConnectorError("LDAP directory search failed; verify Base DN and search filter")
        users=[]
        for row in client.response:
            if row.get("type")!="searchResEntry":
                continue
            attrs=row.get("attributes",{})
            def value(name):
                val=attrs.get(name)
                if isinstance(val,list):
                    val=val[0] if val else None
                return val.hex() if isinstance(val,bytes) else val
            users.append({"dn":row["dn"],"username":value(options.username_attribute),"email":value(options.email_attribute),
                "uid":value(options.uid_attribute),"first_name":value(options.first_name_attribute),
                "last_name":value(options.last_name_attribute),"full_name":value(options.full_name_attribute),
                "phone":value(options.phone_attribute),"home_directory":value(options.home_directory_attribute)})
        return {"users":users[:options.max_entries],"truncated":result_code==4 or len(users)>options.max_entries}


def authenticate(connection,options,username,password):
    if not username or not password:
        raise ConnectorError("LDAP authentication requires a username and nonempty password")
    users=lookup(connection,options,username)["users"]
    if len(users)!=1:
        raise ConnectorError("LDAP authentication failed")
    with session(connection,options,user=users[0]["dn"],password=password):
        return {"authenticated":True,"user":users[0]}
