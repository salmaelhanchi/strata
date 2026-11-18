"""What a robots.txt says, in a form people can read: groups, rules and sitemaps.

Verdicts never come from here: ``HostRobots.verdict`` asks Protego, which implements RFC 9309.
This parser only lists what the file states, for reports (``strata profile``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class RobotsGroup(BaseModel):
    """One group: the user agents it names and the rules that follow them, in file order."""

    model_config = ConfigDict(frozen=True)

    agents: list[str]
    rules: list[tuple[str, str]] = []  # ("allow" | "disallow", path)
    crawl_delay: str | None = None

    def names(self, token: str) -> bool:
        return any(a.lower() == token.lower() for a in self.agents)


class RobotsRules(BaseModel):
    model_config = ConfigDict(frozen=True)

    groups: list[RobotsGroup] = []
    sitemaps: list[str] = []

    def applying_to(self, token: str) -> RobotsGroup | None:
        """The group RFC 9309 applies to ``token``: the one naming it, else the ``*`` group."""
        named = [g for g in self.groups if g.names(token)]
        if named:
            return named[0]
        return next((g for g in self.groups if g.names("*")), None)


def parse_rules(text: str) -> RobotsRules:
    groups: list[RobotsGroup] = []
    agents: list[str] = []
    rules: list[tuple[str, str]] = []
    delay: str | None = None
    sitemaps: list[str] = []
    in_rules = False

    def close() -> None:
        if agents:
            groups.append(RobotsGroup(agents=list(agents), rules=list(rules), crawl_delay=delay))

    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        field, colon, value = line.partition(":")
        if not colon:
            continue
        field, value = field.strip().lower(), value.strip()
        if field == "user-agent":
            if in_rules:  # a user-agent after rules starts the next group
                close()
                agents, rules, delay, in_rules = [], [], None, False
            agents.append(value)
        elif field in ("allow", "disallow") and agents:
            in_rules = True
            rules.append((field, value))
        elif field == "crawl-delay" and agents:
            in_rules = True
            delay = value
        elif field == "sitemap" and value:
            sitemaps.append(value)
    close()
    return RobotsRules(groups=groups, sitemaps=sitemaps)
