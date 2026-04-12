# ICSA-24-046-01: Siemens SCALANCE W Products Vulnerabilities

**Advisory ID:** ICSA-24-046-01

**Release Date:** February 15, 2024

**Severity:** Critical

**CVSS v3 Base Score:** 9.8 / 10.0

---

## Summary

CISA is aware of multiple critical vulnerabilities in Siemens SCALANCE W-700 and W-1700 series
industrial wireless access points. Successful exploitation of these vulnerabilities could allow
an unauthenticated remote attacker to execute arbitrary code, gain full administrative control
of the affected device, or cause a denial-of-service condition. These devices are widely deployed
in industrial control system environments across critical infrastructure sectors including energy,
manufacturing, and water treatment.

---

## Affected Products

The following products are affected:

- Siemens SCALANCE W700 Series (all versions prior to V6.5.0)
- Siemens SCALANCE W1700 Series (all versions prior to V2.0.1)
- Siemens SCALANCE WAM763-1 (all versions prior to V3.0.0)

---

## CVE Identifiers

- CVE-2024-23814
- CVE-2024-23815
- CVE-2024-23816

---

## Vulnerability Details

**CVE-2024-23814 (CVSS 9.8 — Critical):** A stack-based buffer overflow vulnerability exists
in the HTTP request handling component of the SCALANCE W firmware. An unauthenticated attacker
on the same network segment can send a specially crafted HTTP request to trigger the overflow
and execute arbitrary code with root privileges.

**CVE-2024-23815 (CVSS 8.8 — High):** The device management interface does not properly
validate session tokens, allowing session hijacking after an authenticated administrator
has logged in. An attacker with network access could take over an active session.

**CVE-2024-23816 (CVSS 7.5 — High):** Improper input validation in the SNMP daemon allows
an authenticated attacker to cause a denial-of-service condition by sending malformed OID
strings.

---

## Mitigations

Siemens has released firmware updates to address these vulnerabilities. CISA recommends
users apply the following mitigations as soon as possible:

1. Update all affected SCALANCE W-700 devices to firmware version V6.5.0 or later.
2. Update all affected SCALANCE W-1700 devices to firmware version V2.0.1 or later.
3. Update all affected SCALANCE WAM763-1 devices to firmware version V3.0.0 or later.
4. Restrict network access to the device management interface using firewall rules.
5. Disable unused remote management interfaces (HTTP, HTTPS, SNMP) where not required.
6. Monitor industrial network traffic for unusual HTTP patterns targeting these devices.

If immediate patching is not possible, CISA recommends placing affected devices behind
a network firewall or DMZ and implementing strict access controls until patches can be applied.

---

## Recommendations

CISA encourages organizations to perform proper impact analysis and risk assessment prior
to deploying defensive measures. CISA also recommends organizations implement the following
general security practices:

- Minimize network exposure for all industrial control system devices and ensure they are
  not accessible from the internet.
- Locate control system networks and remote devices behind firewalls and isolate them from
  business networks.
- When remote access is required, use secure methods such as Virtual Private Networks (VPNs).

For more information, refer to CISA's ICS security guidance at https://www.cisa.gov/ics.

---

*This advisory was produced by CISA's Industrial Control Systems (ICS) team.*
