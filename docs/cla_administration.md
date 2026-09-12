# Contributor agreement administration

## Policy and rollout status

The intended policy is that **every PR requires a signed CLA covering every
human contributor represented in that PR before merge**. This includes code,
tests, and documentation. Repository membership is not proof of employer
authorization. No contributor's signature should be created by a maintainer
or an automation acting on their behalf.

The [proposed agreement](../CLA.md) is currently a draft for legal review. No
signatures have been solicited, no signing service has been connected, and no
GitHub merge rule has been changed as part of drafting it. Complete the steps
below before announcing that automated enforcement is active.

## Review the agreement

Have qualified counsel review the copyright and patent grants, commercial
relicensing rights, employer authorization, electronic assent, privacy and
record retention, and application across relevant jurisdictions. Confirm the
legal entity's name and decide whether a governing-law provision is appropriate.
The draft deliberately does not invent a jurisdiction or claim that an
individual can automatically license an employer's work.

The draft preserves contributor ownership and permits commercial use and
relicensing. It does not require copyright assignment or a broad contributor
indemnity. Material from earlier PRs requires explicit inclusion; there is no
automatic retroactive signature.

## Configure signing and require the check

Use the hosted [CLA Assistant](https://github.com/cla-assistant/cla-assistant),
which supports GitHub-authenticated signing, agreement versioning, PR statuses,
and exporting signature records. The separate
[CLA Assistant GitHub Action](https://github.com/contributor-assistant/github-action)
is archived and is not the proposed dependency for this setup.

1. Approve the final agreement, remove its draft notice, and assign a stable
   version. Publish that exact version as the Gist required by CLA Assistant.
   Keep an immutable copy and its revision identifier with the agreement records.
2. Sign in to CLA Assistant as an authorized GizmoData repository administrator
   and connect the `gizmodata/gizmosql` repository to that agreement. Review the
   requested GitHub permissions before authorizing the service.
3. Configure the signing form to identify the GitHub account, legal signer, and
   whether they act for an entity. Provide a private route for employer
   authorization and manually signed agreements. Collect only necessary data.
4. Open a test PR from a separate contributor account. Confirm that an unsigned
   author produces a failing CLA status and that signing the exact agreement
   makes it pass. Test a PR with multiple authors and unlinked commit emails;
   do not treat unresolved authors as signed. Verify a newly added unsigned
   author makes a previously passing PR fail again.
5. In the ruleset or branch protection for every merge target, require the
   **actual CLA status context produced by the service**. Bind it to the expected
   integration where GitHub supports that restriction. Require PRs and keep
   bypass permissions limited to documented administrators. Also verify that
   the service supports the repository's merge queue configuration, if used.
6. Confirm an unsigned PR cannot merge through the UI or API. Confirm a signed
   PR still needs the normal code review and CI checks. A CLA signature is not
   approval of the code.
7. Update [CONTRIBUTING.md](../CONTRIBUTING.md) with the active signing link and
   remove its rollout notice. Record the configured status context, ruleset,
   agreement revision, activation date, and administrator in the private
   administration record.

Use individual, reviewed bot exceptions only if needed for automated dependency
updates. Do not exempt all organization members or use wildcard bot exemptions.
Other contributors must sign for themselves. For coauthored or employer-owned
work that automation cannot establish, obtain and record the missing coverage
before any manual override.

## Retain evidence and handle revisions

Export signatures regularly to an access-controlled GizmoData record store,
together with the exact agreement text, revision, acceptance time, and signer
identity. Keep employer approvals and private emails out of public Git commits
and PR comments. Establish retention and privacy practices during legal review.

For a material agreement change, publish a new version and verify that the
service requests new assent for subsequent contributions. Preserve earlier
agreements and signatures. Do not edit an old record to make it appear that a
contributor signed newer terms.

Useful background: Apache distinguishes
[individual and corporate contributor authorization](https://www.apache.org/licenses/contributor-agreements.html).
Its agreements are written for the ASF; the GizmoData draft is a separate proposal
and should be reviewed for GizmoData's own business and licensing model.
