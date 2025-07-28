# iceberg-go fork

This fork of [iceberg-go](https://github.com/apache/iceberg-go) exists because it has a dependency on 
[gocloud.dev](https://github.com/google/go-cloud) which has been problematic enough with many API breaking changes
that blocked important dependency upgrades in repos like [dd-go](https://github.com/DataDog/dd-go) and 
[dd-source](https://github.com/DataDog/dd-source). 

Because of this, [the guideline](https://datadoghq.atlassian.net/wiki/spaces/ENG/pages/5335876583/Blob+storage+in+Go) 
was put in place for anything that depends on gocloud.dev to vendor that dependency and
prevent it leaking through in our main repositories. 

# Synch with upstream
This README comes with tooling that will:
* Add the `go mod replace` directive to allow this fork to compile without any iceberg-go import changes
* Add `gocloud.dev` as a git subtree 
* Rewrite `gocloud.dev` import paths

This commit should include all that's needed to recreate a working fork from a fresh copy of iceberg-go. The idea being
that we could always just `git reset --hard` from the upstream repository, cherry-pick this commit and re-run the
`./initialize-fork.sh` to get the repo back to a up-to-date working state. Note that this approach would involve 
rewriting git history and might be reserved in case the merging/rebasing from upstream is too messy or involved.

To find the fork-initializing commit, look up the commit by the tag:
```shell
git rev-list -n 1 fork-initialization
```

# The Path To Irrelevance

If [iceberg-go](https://github.com/apache/iceberg-go) ever breaks free from [gocloud.dev](https://github.com/google/go-cloud), we could potentially remove the 
need for this fork and go back to the upstream repo.