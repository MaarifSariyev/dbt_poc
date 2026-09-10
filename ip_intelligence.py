"""
Real, offline IP intelligence for VPN / datacenter detection.

Data source: X4BNet/lists_vpn (https://github.com/X4BNet/lists_vpn), a
community-maintained, daily-updated list of VPN provider and datacenter
IPv4 CIDR ranges. This is the same category of signal commercial fraud
APIs (IPQualityScore, MaxMind GeoIP2 Anonymous IP, IP2Proxy) sell as a
service; here it is bundled as flat files and refreshed manually or via
`manage.py refresh_ip_lists`.

This module answers two independent questions for a given client IP:
  1. is it inside a known VPN-provider range?
  2. is it inside a known datacenter/hosting range (broader than VPN --
     includes cloud providers, colo, etc.)?

Both lists are parsed once per process into sorted integer-range tables
and looked up with binary search (bisect), so a lookup against ~43k
datacenter ranges plus ~11k VPN ranges costs O(log n) integer
comparisons rather than a linear scan or per-request network fetch.

Real (non-fixture) IPs that hit neither list are treated as ordinary
residential/mobile IPs -- the common case for legitimate players.
"""
from __future__ import annotations

import ipaddress
import os
from bisect import bisect_right
from dataclasses import dataclass
from functools import lru_cache

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
VPN_LIST_PATH = os.path.join(DATA_DIR, "vpn_ipv4.txt")
DATACENTER_LIST_PATH = os.path.join(DATA_DIR, "datacenter_ipv4.txt")


def _load_ranges(path: str) -> list[tuple[int, int]]:
    """Parse a newline-delimited CIDR file into sorted (start, end) int ranges."""
    ranges = []
    if not os.path.exists(path):
        return ranges
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                net = ipaddress.ip_network(line, strict=False)
            except ValueError:
                continue
            if net.version != 4:
                continue
            ranges.append((int(net.network_address), int(net.broadcast_address)))
    ranges.sort()
    return ranges


@lru_cache(maxsize=1)
def _vpn_ranges() -> tuple[list[int], list[tuple[int, int]]]:
    ranges = _load_ranges(VPN_LIST_PATH)
    starts = [r[0] for r in ranges]
    return starts, ranges


@lru_cache(maxsize=1)
def _datacenter_ranges() -> tuple[list[int], list[tuple[int, int]]]:
    ranges = _load_ranges(DATACENTER_LIST_PATH)
    starts = [r[0] for r in ranges]
    return starts, ranges


def _in_ranges(ip_int: int, starts: list[int], ranges: list[tuple[int, int]]) -> bool:
    if not ranges:
        return False
    idx = bisect_right(starts, ip_int) - 1
    if idx < 0:
        return False
    start, end = ranges[idx]
    return start <= ip_int <= end


@dataclass
class IPIntelligence:
    ip: str
    is_vpn: bool
    is_datacenter: bool
    valid: bool  # False if the input wasn't a parseable IPv4 address

    @property
    def network_type(self) -> str:
        if self.is_vpn:
            return "vpn"
        if self.is_datacenter:
            return "datacenter"
        return "residential"

    @property
    def risk_contribution(self) -> int:
        """0-100 baseline risk purely from network type, before other signals."""
        if self.is_vpn:
            return 90
        if self.is_datacenter:
            return 65
        return 5


def lookup_ip(ip: str) -> IPIntelligence:
    """
    Classify an IPv4 address against the bundled VPN/datacenter range lists.

    Unknown/unparseable input (e.g. IPv6, empty string from a test client)
    is returned as valid=False with conservative low-risk defaults, rather
    than raising -- checkout should never 500 because of a malformed IP.
    """
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return IPIntelligence(ip=ip, is_vpn=False, is_datacenter=False, valid=False)

    if addr.version != 4:
        # IPv6 not covered by the bundled lists yet.
        return IPIntelligence(ip=ip, is_vpn=False, is_datacenter=False, valid=False)

    ip_int = int(addr)
    vpn_starts, vpn_ranges = _vpn_ranges()
    dc_starts, dc_ranges = _datacenter_ranges()

    is_vpn = _in_ranges(ip_int, vpn_starts, vpn_ranges)
    is_datacenter = is_vpn or _in_ranges(ip_int, dc_starts, dc_ranges)

    return IPIntelligence(ip=ip, is_vpn=is_vpn, is_datacenter=is_datacenter, valid=True)


def list_stats() -> dict:
    """Small introspection helper, useful for a health-check endpoint or admin page."""
    vpn_starts, _ = _vpn_ranges()
    dc_starts, _ = _datacenter_ranges()
    return {"vpn_ranges": len(vpn_starts), "datacenter_ranges": len(dc_starts)}
