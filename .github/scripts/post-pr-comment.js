// Shared PR comment posting utility.
// Used by clang-tidy-comment.yml.
//
// Usage from actions/github-script:
//   const post = require('./.github/scripts/post-pr-comment.js');
//   await post({ github, context, core, prNumber, body, marker });
//
// Parameters:
//   github   - octokit instance from actions/github-script
//   context  - GitHub Actions context
//   core     - @actions/core for logging
//   prNumber - PR number to comment on
//   body     - comment body (markdown string)
//   marker   - HTML comment marker for dedup (e.g. '<!-- clang-tidy-bot -->')
//              If an existing comment with this marker is found, it is updated.
//              If not found, a new comment is created.

module.exports = async function postPrComment(
    {github, context, core, prNumber, body, marker}) {
  if (!prNumber || !body) {
    core.warning('Missing prNumber or body; skipping comment.');
    return;
  }

  const owner = context.repo.owner;
  const repo = context.repo.repo;

  // Ensure marker is embedded in the body
  const markedBody = body.includes(marker) ? body : `${marker}\n${body}`;

  async function listComments() {
    return await github.paginate(github.rest.issues.listComments, {
      owner,
      repo,
      issue_number: prNumber,
      per_page: 100,
    });
  }

  const comments = await listComments();
  const existing = comments.find(
      comment =>
          typeof comment.body === 'string' && comment.body.includes(marker));
  let updated = false;

  if (existing) {
    try {
      await github.rest.issues.updateComment({
        owner,
        repo,
        comment_id: existing.id,
        body: markedBody,
      });
      updated = true;
      core.info(`Updated existing comment ${existing.id}`);
    } catch (error) {
      if (error.status !== 404) {
        throw error;
      }
      core.info(
          `Comment ${existing.id} disappeared before update; ` +
          'creating a fresh comment instead.');
    }
  }

  if (!updated) {
    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: prNumber,
      body: markedBody,
    });
    core.info('Created new PR comment');
  }
};
