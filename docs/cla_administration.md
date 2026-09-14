# Contributor agreement administration

## Policy and rollout status

The policy is that **every PR requires a signed CLA covering every human
contributor represented in that PR before merge**. This includes code, tests,
and documentation. Repository membership is not proof of employer
authorization. No contributor's signature should be created by a maintainer
or an automation acting on their behalf.

There are two agreements, following the model used by many commercial open
source maintainers:

- the [Individual CLA](../CLA.md), the Apache Software Foundation's Individual
  Contributor License Agreement (v2.2) adapted for GizmoData LLC, for people
  contributing on their own behalf; and
- the [Corporate CLA](../CLA-CORPORATE.md), the Apache Software Grant and
  Corporate Contributor License Agreement adapted for GizmoData LLC, signed by
  an employer that designates the employees (Schedule A) who may contribute.

Both keep contributor ownership and grant GizmoData and downstream recipients
a copyright and patent license, with the Apache patent-retaliation clause.
Relative to the Apache originals, only the ASF-specific nonprofit clause and
Apache-id fields were removed and GitHub usernames were added to the signature
records. Enforcement is automated by the `CLA` workflow and the `main`
ruleset described below.

## Review the agreement

Have qualified counsel confirm the adaptation, the legal entity's name, and
whether a governing-law provision is appropriate. The agreements deliberately
do not invent a jurisdiction. Material from earlier PRs is covered only when
identified in a Corporate CLA's Schedule B or a separate written grant; there
is no automatic retroactive signature.

## How enforcement works

`.github/workflows/cla.yml` runs on every pull request (`pull_request_target`,
so fork PRs get a bot comment without executing their code) and whenever a
comment is posted on one. It:

1. Collects every author of the pull request: the PR author plus the GitHub
   account linked to each commit. A commit whose author email is not linked to
   a GitHub account is reported and blocks the check until the contributor
   links the address or amends the commit; unresolved authors are never
   treated as signed.
2. Reads `signatures/cla.json` on the `cla-signatures` branch. Accounts in the
   workflow's `ALLOWLIST` (GizmoData maintainers and bots) never need to sign.
3. Records any author who has commented the exact signing sentence on that
   pull request, appending `{login, date, pull_request, version}` to the file
   with a commit on the `cla-signatures` branch.
4. Posts or updates one bot comment with the outcome and instructions, and
   sets a `CLA` commit status (success or failure) on the PR head commit. The
   status is what the ruleset requires, so a signature by comment turns the
   same pull request green without a new push.

The `main` ruleset requires the `CLA` status context on every pull request
into `main`. Repository administrators may bypass it so direct release pushes
and tag pushes are unaffected; nobody else can merge an unsigned PR through
the UI or the API. A CLA signature is not approval of the code: review and CI
still apply.

Corporate signers do not comment. After the completed Corporate CLA arrives at
info@gizmodata.com, a maintainer adds each Schedule A GitHub username to
`signatures/cla.json` on the `cla-signatures` branch (a normal commit to that
branch) with the corporation name in a `corporation` field, and files the
signed agreement in the private record store. Remove a username from the file
when the corporation revokes that employee's designation.

To change the agreement materially, bump the version in `CLA.md` and in the
`version` field of a fresh `signatures/cla.json`; keep the previous file in the
branch history. Existing signers are then asked to sign again on their next
pull request.

## Retain evidence and handle revisions

Export signatures regularly to an access-controlled GizmoData record store,
together with the exact agreement text, revision, acceptance time, and signer
identity. Keep employer approvals and private emails out of public Git commits
and PR comments. Establish retention and privacy practices during legal review.

For a material agreement change, publish a new version and verify that the
service requests new assent for subsequent contributions. Preserve earlier
agreements and signatures. Do not edit an old record to make it appear that a
contributor signed newer terms.

Useful background: Apache's
[contributor agreements page](https://www.apache.org/licenses/contributor-agreements.html)
explains the individual and corporate split these agreements follow.
