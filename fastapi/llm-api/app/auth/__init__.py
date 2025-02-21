import ldap


class LDAP:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        base_dn: str,
        search_filter: str,
        fields: list,
    ):
        self.host = host
        self.username = username
        self.password = password
        self.base_dn = base_dn
        self.search_filter = search_filter
        self.fields = fields

    def _initialize(self):
        return ldap.initialize(f"ldap://{self.host}")

    def bind_user(self, username: str, password: str) -> bool:
        conn = self._initialize()

        try:
            conn.simple_bind_s(f"{username}@{self.host}", password)
            conn.unbind()
        except ldap.INVALID_CREDENTIALS:
            conn.unbind()
            return False
        return True

    def get_user_info(self, username: str) -> dict | None:
        conn = self._initialize()
        conn.simple_bind_s(f"{self.username}@{self.host}", self.password)

        result = conn.search_s(
            base=self.base_dn,
            scope=ldap.SCOPE_SUBTREE,
            filterstr=self.search_filter.format(user=username),
            attrlist=self.fields,
        )
        conn.unbind()

        if result:
            result: dict = result[0][1]
        else:
            return

        ldap_groups = result.get("memberOf") or []
        user_groups = []
        for cn in ldap_groups:
            groups = [
                group[3:] for group in cn.decode().split(",") if "CN=" in group or "OU=" in group
            ]
            user_groups.extend(groups)

        user_info = dict()
        user_info["groups"] = set(user_groups)

        for field in self.fields:
            user_info[field] = result[field][0].decode()

        return user_info
