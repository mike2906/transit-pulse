# Private ops repo template

These workflows read and write the private archive, so they run in a **private** repo, never
here: a public repo's logs and artifacts are public. Copy `.github/workflows/*.yml` into the
private repo unchanged. They check out the public transit-pulse code at `main` and keep only
credentials and data access in the private repo.

Setup and what each workflow does: [docs/ARCHIVE.md](../../docs/ARCHIVE.md).
