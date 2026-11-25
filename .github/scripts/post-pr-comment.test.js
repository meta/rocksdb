const test = require('node:test');
const assert = require('node:assert/strict');

const postPrComment = require('./post-pr-comment.js');

const MARKER = '<!-- clang-tidy-bot -->';

function makeComment(id, body) {
  return {id, body};
}

function createHarness(initialComments, options = {}) {
  let comments = initialComments.map(comment => ({...comment}));
  let nextCommentId = 1000;

  const calls = {
    update: [],
    create: [],
  };

  const github = {
    paginate: async () => comments.map(comment => ({...comment})),
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
          comments[index] = {...comments[index], body};
          return {data: {...comments[index]}};
        },
        createComment: async ({issue_number, body}) => {
          calls.create.push({issue_number, body});
          const created = makeComment(nextCommentId++, body);
          comments.push(created);
          return {data: {id: created.id}};
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

const context = {
  repo: {
    owner: 'meta',
    repo: 'rocksdb',
  },
};

test('creates a marked comment when none exists', async () => {
  const harness = createHarness([makeComment(1, 'unrelated comment')]);

  await postPrComment({
    github: harness.github,
    context,
    core: createCore(),
    prNumber: 123,
    body: 'first body',
    marker: MARKER,
  });

  assert.equal(harness.calls.update.length, 0);
  assert.equal(harness.calls.create.length, 1);
  assert.equal(harness.calls.create[0].issue_number, 123);
  assert.equal(harness.calls.create[0].body, `${MARKER}\nfirst body`);
  assert.equal(
      harness.getComments().find(comment => comment.id === 1).body,
      'unrelated comment');
});

test('updates the existing marked comment in place', async () => {
  const harness = createHarness([
    makeComment(1, 'unrelated comment'),
    makeComment(2, `${MARKER}\nold body`),
  ]);

  await postPrComment({
    github: harness.github,
    context,
    core: createCore(),
    prNumber: 123,
    body: 'refreshed body',
    marker: MARKER,
  });

  assert.deepEqual(harness.calls.update.map(call => call.comment_id), [2]);
  assert.equal(harness.calls.create.length, 0);
  assert.equal(
      harness.getComments().find(comment => comment.id === 2).body,
      `${MARKER}\nrefreshed body`);
});

test(
    'creates a new comment if the target disappears before update',
    async () => {
      const missing = new Error('gone');
      missing.status = 404;
      const harness = createHarness([makeComment(10, `${MARKER}\nold body`)], {
        updateErrors: {
          10: missing,
        },
      });

      await postPrComment({
        github: harness.github,
        context,
        core: createCore(),
        prNumber: 123,
        body: 'replacement body',
        marker: MARKER,
      });

      assert.equal(harness.calls.update.length, 1);
      assert.equal(harness.calls.create.length, 1);
      assert.equal(harness.calls.create[0].body, `${MARKER}\nreplacement body`);
    });
