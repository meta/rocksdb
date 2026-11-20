const test = require('node:test');
const assert = require('node:assert/strict');

const postPrComment = require('./post-pr-comment.js');

const OBSOLETE_MARKER = '<!-- claude-review-obsolete -->';
const OBSOLETE_TITLE = 'Claude Code Review - OBSOLETE';

function makeComment(id, body, createdAt, updatedAt) {
  return {
    id,
    body,
    created_at: createdAt,
    updated_at: updatedAt || createdAt,
  };
}

function createHarness(initialComments, options = {}) {
  let comments = initialComments.map(comment => ({...comment}));
  let nextCommentId = options.nextCommentId || 1000;
  let paginateCount = 0;

  const calls = {
    update: [],
    create: [],
    delete: [],
  };

  const github = {
    paginate: async () => {
      paginateCount++;
      if (options.onPaginate) {
        const updated = options.onPaginate({
          paginateCount,
          comments: comments.map(comment => ({...comment})),
        });
        if (updated) {
          comments = updated.map(comment => ({...comment}));
        }
      }
      return comments.map(comment => ({...comment}));
    },
    rest: {
      issues: {
        listComments: () => {
          throw new Error('listComments should only be used through paginate');
        },
        updateComment: async ({comment_id, body}) => {
          calls.update.push({comment_id, body});
          const error =
              options.updateErrors && options.updateErrors[comment_id];
          if (error) {
            throw error;
          }
          const index =
              comments.findIndex(comment => comment.id === comment_id);
          if (index === -1) {
            const notFound = new Error(`Comment ${comment_id} not found`);
            notFound.status = 404;
            throw notFound;
          }
          const updated = {
            ...comments[index],
            body,
            updated_at: options.updateTimestamp || '2026-04-24T00:00:00Z',
          };
          comments[index] = updated;
          return {data: {...updated}};
        },
        createComment: async ({issue_number, body}) => {
          calls.create.push({issue_number, body});
          const created = makeComment(
              nextCommentId++, body,
              options.createTimestamp || '2026-04-24T00:00:00Z');
          comments.push(created);
          return {data: {id: created.id}};
        },
        deleteComment: async ({comment_id}) => {
          calls.delete.push({comment_id});
          const error =
              options.deleteErrors && options.deleteErrors[comment_id];
          if (error) {
            throw error;
          }
          comments = comments.filter(comment => comment.id !== comment_id);
        },
      },
    },
  };

  return {
    github,
    calls,
    getComments: () => comments.map(comment => ({...comment})),
  };
}

function createCore() {
  return {
    info: () => {},
    warning: () => {},
  };
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function assertObsoleteComment(comment, originalBody) {
  assert.ok(comment);
  assert.match(comment.body, new RegExp(escapeRegExp(OBSOLETE_MARKER)));
  assert.match(comment.body, new RegExp(`## ${escapeRegExp(OBSOLETE_TITLE)}`));
  assert.match(comment.body, /<details>/);
  assert.match(comment.body, /Superseded by a newer AI review/);
  assert.match(comment.body, new RegExp(escapeRegExp(originalBody)));
}

const context = {
  repo: {
    owner: 'meta',
    repo: 'rocksdb',
  },
};

test(
    'creates a fresh review comment and supersedes legacy comments',
    async () => {
      const harness = createHarness(
          [
            makeComment(
                1, '<!-- claude-review-auto -->\nold legacy', 'not-a-date'),
            makeComment(
                2, '<!-- claude-review-auto -->\nnew legacy',
                '2026-04-21T00:00:00Z'),
          ],
          {
            updateTimestamp: '2026-04-24T00:00:00Z',
          });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 14659,
        body: 'review body',
        marker: '<!-- claude-review-auto-run-500 -->',
        legacyMarkers: ['<!-- claude-review-auto -->'],
        prunePrefix: '<!-- claude-review-auto-',
        preserveLatest: 1,
        obsoleteMarker: OBSOLETE_MARKER,
        obsoleteTitle: OBSOLETE_TITLE,
      });

      assert.equal(harness.calls.create.length, 1);
      assert.deepEqual(
          harness.calls.update.map(call => call.comment_id), [2, 1]);
      assert.equal(harness.calls.delete.length, 0);

      const comments = harness.getComments();
      assert.equal(comments.length, 3);
      assert.match(
          comments.find(comment => comment.id === 1000).body,
          /<!-- claude-review-auto-run-500 -->/);
      assertObsoleteComment(
          comments.find(comment => comment.id === 2),
          '<!-- claude-review-auto -->\nnew legacy');
      assertObsoleteComment(
          comments.find(comment => comment.id === 1),
          '<!-- claude-review-auto -->\nold legacy');
    });

test(
    'updates an exact-match comment and supersedes leftover legacy comments',
    async () => {
      const harness = createHarness(
          [
            makeComment(
                3, '<!-- claude-review-auto-abcdef0 -->\ncurrent body',
                '2026-04-24T00:00:00Z'),
            makeComment(
                4, '<!-- claude-review-auto -->\nlegacy body',
                '2026-04-20T00:00:00Z'),
          ],
          {
            updateTimestamp: '2026-04-24T01:00:00Z',
          });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 14659,
        body: 'refreshed body',
        marker: '<!-- claude-review-auto-abcdef0 -->',
        legacyMarkers: ['<!-- claude-review-auto -->'],
        prunePrefix: '<!-- claude-review-auto-',
        preserveLatest: 1,
        obsoleteMarker: OBSOLETE_MARKER,
        obsoleteTitle: OBSOLETE_TITLE,
      });

      assert.deepEqual(
          harness.calls.update.map(call => call.comment_id), [3, 4]);
      assert.equal(harness.calls.create.length, 0);
      assert.equal(harness.calls.delete.length, 0);

      const comments = harness.getComments();
      assert.match(
          comments.find(comment => comment.id === 3).body,
          /<!-- claude-review-auto-abcdef0 -->\nrefreshed body/);
      assertObsoleteComment(
          comments.find(comment => comment.id === 4),
          '<!-- claude-review-auto -->\nlegacy body');
    });

test(
    'creates a new comment if the target disappears before update',
    async () => {
      const missing = new Error('gone');
      missing.status = 404;
      const harness = createHarness(
          [
            makeComment(
                10, '<!-- claude-review-auto-abcdef0 -->\nold body',
                '2026-04-20T00:00:00Z'),
          ],
          {
            updateErrors: {
              10: missing,
            },
          });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 14659,
        body: 'replacement body',
        marker: '<!-- claude-review-auto-abcdef0 -->',
      });

      assert.equal(harness.calls.update.length, 1);
      assert.equal(harness.calls.create.length, 1);
      assert.match(
          harness.calls.create[0].body, /<!-- claude-review-auto-abcdef0 -->/);
    });

test(
    'ignores 404 when superseding a comment already deleted by another run',
    async () => {
      const missing = new Error('gone');
      missing.status = 404;
      const harness = createHarness(
          [
            makeComment(
                20, '<!-- claude-review-auto-oldest -->\noldest',
                '2026-04-20T00:00:00Z'),
            makeComment(
                21, '<!-- claude-review-auto-newer -->\nnewer',
                '2026-04-21T00:00:00Z'),
          ],
          {
            nextCommentId: 30,
            createTimestamp: '2026-04-24T00:00:00Z',
            deleteErrors: {
              20: missing,
            },
          });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 14659,
        body: 'fresh body',
        marker: '<!-- claude-review-auto-latest -->',
        prunePrefix: '<!-- claude-review-auto-',
        preserveLatest: 1,
        obsoleteMarker: OBSOLETE_MARKER,
        obsoleteTitle: OBSOLETE_TITLE,
      });

      assert.equal(harness.calls.create.length, 1);
      assert.equal(harness.calls.delete.length, 0);
      assert.deepEqual(
          harness.calls.update.map(call => call.comment_id), [21, 20]);

      const comments = harness.getComments();
      assertObsoleteComment(
          comments.find(comment => comment.id === 21),
          '<!-- claude-review-auto-newer -->\nnewer');
    });

test(
    'supersedes the current review when a newer concurrent one appears',
    async () => {
      const harness = createHarness(
          [
            makeComment(
                40, '<!-- claude-review-auto-current -->\ncurrent',
                '2026-04-20T00:00:00Z'),
            makeComment(
                41, '<!-- claude-review-auto-older -->\nolder',
                '2026-04-19T00:00:00Z'),
          ],
          {
            updateTimestamp: '2026-04-24T00:00:00Z',
            onPaginate: ({paginateCount, comments}) => {
              if (paginateCount !== 2) {
                return comments;
              }
              return comments.concat([
                makeComment(
                    42, '<!-- claude-review-auto-newer -->\nnewer',
                    '2026-04-25T00:00:00Z'),
              ]);
            },
          });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 14659,
        body: 'updated current body',
        marker: '<!-- claude-review-auto-current -->',
        prunePrefix: '<!-- claude-review-auto-',
        preserveLatest: 1,
        obsoleteMarker: OBSOLETE_MARKER,
        obsoleteTitle: OBSOLETE_TITLE,
      });

      assert.deepEqual(
          harness.calls.update.map(call => call.comment_id), [40, 40, 41]);
      assert.equal(harness.calls.delete.length, 0);

      const comments = harness.getComments();
      assert.match(
          comments.find(comment => comment.id === 42).body,
          /<!-- claude-review-auto-newer -->\nnewer/);
      assertObsoleteComment(
          comments.find(comment => comment.id === 40),
          '<!-- claude-review-auto-current -->\nupdated current body');
      assertObsoleteComment(
          comments.find(comment => comment.id === 41),
          '<!-- claude-review-auto-older -->\nolder');
    });
