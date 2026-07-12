# Preserved Y Snapshot SHA-256 (2026-07-13)

These hashes identify the one-time file-level preservation copy made from the
RaiDrive-backed `Y:` worktree into the local NTFS clone after the storage
incident.  No later build, test, Git, or deployment step may depend on `Y:`.

```text
CHANGELOG_claude.md                      5511fd318fdde9dd47cc6aa346d025885f45182edbd1ade523465026a0d2502f
README.md                                dcb10a498c28c4609d371120f3704576523b723b8515725401c7278264d65ebe
config\app.example.yaml                  463f075780ef0a8a7a5d9dc6e16b51573c1e99f41e7422cc123da40600e86cb8
slurm_scheduler\__main__.py              5565a3bb6e4ec876829f6bcd77903e3b6d8bac09ca6d67b9d19d00e8384dec9f
slurm_scheduler\app.py                   9ec504b675e2e80ef8653fe5f7c5ced11df8b02385ecf9cacf54d9d04ca3fce5
slurm_scheduler\config.py                f8c389246cca7c61883dce4af484b111de908b3c6c286a105958182c52108e9a
slurm_scheduler\db.py                    d8e6f5acbe84cfa600ff5d119a281760dfa53f51ea0542ca2443b470748a6d82
slurm_scheduler\scheduler.py             cdb313f1cbf40204031627fc81dfb171190baff9f4cb0b3b056fcf014c426d11
slurm_scheduler\slurm.py                 3c1966761245ab3c28f7ce21285758ba9ae5ccfaa3b1482defe2422badf48381
templates\base.html                      060201bf9f1a9edc8a5b54aa2807c205cbb1340817ff4e425b3f801e565b9761
tests\test_core.py                       4307f1971c9fb2502b73c3727097b8e2a5d8946deccf1d5670517e6e6e8ff5ba
docs\aedt_pool.md                        c0a923e2d5932d5ef135cacf45216499e85ee873de9aa3aa7fc90c86daa7e0f7
docs\aedt_pool_runbook.md                e87b2dfa743c946089ea032a5aeec065b358631ac7248ebae6e5be2e6323bd3f
docs\incident_web_listener_winerror64.md 2d16c565438437d926ceca39f9e4114afadda1e715e594e4875352571bc49557
scripts\aedt_pool_fault_injection.py     a8bd054cc139e39f3dff2947919f4a17bce3c85bfec1e19c590f3a617df66b2e
slurm_scheduler\aedt_attach_client.py    37be508cfb1c8caab823fa7662d078bc40df4cd1cad6c5d5bd4219b50e6a72a4
slurm_scheduler\aedt_pool.py             4971d5b53aad01b9e51f225410048424b3dda66a03e070df8e1d5e44a1bfbf24
slurm_scheduler\aedt_pool_api.py         c3e69cebcbc3b40189950e399bb9468f07b32b0c98bb4e781d2ee68180f759db
slurm_scheduler\aedt_session_host.py     4b04e3d47a2a107abe3697646fe79f4d23921ccf94b304bd3b75271246a4a53a
slurm_scheduler\web_supervisor.py        77b87c903d0f42b2b3d0fad8072f345e4d5661c1810922c0aeb3e4659e7b36c0
templates\aedt_pool.html                 bfe69bc829484e533068d5d1e48477c1c6c5d306f4cf9dec8c4d4d82c60260ea
tests\test_aedt_pool.py                  d62181bc8098fbefddeffea887903e97f1eb0e3dca273a6e81522a2ecc86bc94
tests\test_web_supervisor.py             c803e40631e1bb2fea7b57e41002788bca4b5401adfe170809d8f84a5964abf3
```

The hashes above intentionally describe the preserved input snapshot.  The
documentation files have since been updated in the NTFS clone, so their current
hashes are expected to differ.
