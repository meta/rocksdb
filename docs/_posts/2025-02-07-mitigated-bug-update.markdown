---
title: Addressing a Mitigated Misconfig Bug in the RocksDB OSS Repository
layout: post
author: hx235
category: blog
---

Dear RocksDB Community,

We want to share an update about the bug that allowed our bug bounty researcher to update the release note title in August 2024 involving the RocksDB open-source repository on GitHub. This issue was found and responsibly disclosed to us by an external bug bounty researcher through our [Meta Bug Bounty program](https://www.facebook.com/whitehat) and quickly mitigated by our teams. We have not seen any evidence of malicious exploitation. Please note that no action is required from our community, as we have taken all necessary steps to remediate the issue.

## Background

RocksDB is a high-performance storage engine library widely used in various large-scale applications. On August 21, 2024, a bug was reported to us by one of our  bug bounty researchers. They were able to demonstrate the ability to obtain the GITHUB_TOKEN used in GitHub Actions workflows. This token provides write access to the metadata of the repository, and the researcher used it to change the title of the release note 9.5.2 as proof of concept. The researcher also unsuccessfully attempted to merge a change to the main branch of the repository; however, we had access controls set up to prevent it from going through.

## Key Details

- **Incident Discovery**: After the bug bounty researcher changed the open source release note title to demonstrate the vulnerability, external users noticed this change and [notified](https://github.com/meta/rocksdb/issues/12962) RocksDB. RocksDB then reached out to the Bug Bounty program to confirm this was the result of security research.
- **No Malicious Abuse**: The investigation confirmed that no code or data was compromised. The change was public and visible on GitHub.
- **Tag Reversion Clarification**: On August 21, a tag named "v9.5.2" was initially published pointing to an incorrect commit. This was unrelated to the bug described here and was promptly corrected by pointing the tag to the correct commit. The release binary remains safe to use, and this correction does not impact the security or integrity of the release.

We’ve taken the following steps to mitigate and remediate the issue:
- The release note title was corrected.
- The workflow running on the self-hosted runner was disabled immediately.
- It was confirmed that the GITHUB_TOKEN expired and is no longer in use.
- The binary tagged for public release was examined to confirm that it was not compromised.
- Action logs were cross-checked to ensure no other actions were taken with the compromised token, other than the release note title change and the failed attempts to merge self-approved pull requests to the main branch.
- We have [scoped down](https://github.com/meta/rocksdb/pull/12973) the access level of tokens generated for workflows to prevent similar issues. Additionally, we are developing better guidelines for bug bounty researchers to minimize disruptions during their research.


Thank you for your continued support and trust in RocksDB.


Sincerely,

The RocksDB Team
