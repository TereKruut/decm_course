# Git Guide (Lecture 3, UI-First)

## Learning Approach

For this lecture, use VS Code Source Control UI as the primary path.
Command line equivalents are included for intuition and troubleshooting.

## Core Terminology

1. Repository: project with full history
2. Commit: snapshot with message
3. Branch: separate line of work
4. Remote: hosted repository (for example GitHub)
5. Pull Request: proposed change from one branch to another

Reference:

- GitHub Flow: <https://docs.github.com/en/get-started/using-github/github-flow>
- Dedicated troubleshooting for VS Code auth/account/config issues: [Git Troubleshooting](./git-troubleshooting.md)

## VS Code UI Workflow (Primary)

### 1) Create or switch branch

1. Click branch name in bottom-left status bar.
2. Select `Create new branch...`.
3. Name it (example: `lecture1-myname`).

### 2) Make a change

1. Edit a Markdown file.
2. Save file.
3. Open Source Control view.

### 3) Review and stage

1. Click changed file to open side-by-side diff.
2. Click `+` on file (or `Stage All Changes`).

### 4) Commit

1. Write message in Source Control input box.
2. Click `Commit`.

Suggested message style:

1. `docs: add lecture 3 notes`
2. `fix: update docker command example`

### 5) Push and sync

1. Click `Sync Changes` or `Publish Branch`.
2. Authenticate with GitHub if prompted.

Reference:

- VS Code Source Control quickstart: <https://code.visualstudio.com/docs/sourcecontrol/quickstart>

## CLI Equivalence Table

| VS Code UI action | CLI equivalent |
| --- | --- |
| Create branch | `git switch -c lecture1-myname` |
| View status | `git status` |
| Stage file | `git add <file>` |
| Commit | `git commit -m "message"` |
| Push branch | `git push -u origin lecture1-myname` |

## Minimal Conflict Recovery

If push is rejected:

```bash
git pull --rebase
git push
```

If merge conflicts appear in VS Code:

1. Open conflicted file from Source Control.
2. Use inline conflict actions (`Accept Current`, `Accept Incoming`, `Accept Both`) carefully.
3. Save, stage, commit, and push again.

## Optional Deep-Dive

1. Compare `merge` vs `rebase` conceptually.
2. Try a small PR review using GitHub web UI.
3. Review [Git Troubleshooting](./git-troubleshooting.md) and practice one failure recovery.
