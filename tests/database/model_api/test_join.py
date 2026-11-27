"""Model API join tests.

A join never puts a related instance inside a typed dataclass; it returns typed
tuples, each side built from its own slice of the nested row.
"""

# pylint: disable=redefined-outer-name,invalid-name,missing-function-docstring,missing-class-docstring
from pytest import fixture, raises

from luminadb import BaseModel, Database, Foreign, Primary, model

from ..setup import setup_model_api


@fixture
def models():
    database = Database(":memory:")
    Users, Posts = setup_model_api(database)
    Users.create(id="u1", username="alice")
    Users.create(id="u2", username="bob")  # no posts
    Posts.create(id="p1", user_id="u1", title="first", content="c")
    Posts.create(id="p2", user_id="u1", title="second", content="c")
    return database, Users, Posts


def test_join_returns_typed_pairs_and_infers_on_from_foreign(models):
    _, Users, Posts = models
    pairs = Posts.join(Users).order_by("id").fetch()  # on inferred from Foreign()
    assert [(p.id, u.id) for p, u in pairs] == [("p1", "u1"), ("p2", "u1")]
    post, user = pairs[0]
    assert isinstance(post, Posts) and isinstance(user, Users)


def test_inference_works_in_the_reverse_direction(models):
    _, Users, Posts = models
    pairs = Users.join(Posts).order_by("id").fetch()  # the FK lives on Posts
    assert [(u.id, p.id) for u, p in pairs] == [("u1", "p1"), ("u1", "p2")]


def test_join_issues_a_single_statement(models):
    database, Users, Posts = models
    seen: list[str] = []
    database.sql.set_trace_callback(seen.append)
    try:
        Posts.join(Users).fetch()
    finally:
        database.sql.set_trace_callback(None)
    assert len(seen) == 1  # columns come from the dataclass fields: no metadata query


def test_left_join_gives_none_for_the_missing_side(models):
    _, Users, Posts = models
    pairs = Users.join(Posts, type="left").order_by("id").order_by("posts.id").fetch()
    assert [(u.id, p.id if p else None) for u, p in pairs] == [
        ("u1", "p1"),
        ("u1", "p2"),
        ("u2", None),
    ]


def test_group_one_to_many(models):
    _, Users, Posts = models
    grouped = Users.join(Posts, type="left").order_by("id").order_by("posts.id").group()
    assert [(u.id, [p.id for p in posts]) for u, posts in grouped] == [
        ("u1", ["p1", "p2"]),
        ("u2", []),
    ]
    assert all(isinstance(p, Posts) for _, posts in grouped for p in posts)


def test_group_with_inner_join_drops_parents_without_children(models):
    _, Users, Posts = models
    grouped = Users.join(Posts).group()
    assert [u.id for u, _ in grouped] == ["u1"]


def test_group_requires_inner_or_left(models):
    _, Users, Posts = models
    with raises(ValueError):
        Users.join(Posts, type="cross").group()


def test_where_bare_key_is_base_model_and_qualified_reaches_the_other(models):
    _, Users, Posts = models
    pairs = Users.join(Posts).where(username="alice", **{"posts.title": "second"}).fetch()
    assert [(u.id, p.id) for u, p in pairs] == [("u1", "p2")]


def test_fetch_one(models):
    _, Users, Posts = models
    pair = Posts.join(Users).where(id="p2").fetch_one()
    assert pair is not None and pair[0].id == "p2" and pair[1].id == "u1"
    assert Posts.join(Users).where(id="nope").fetch_one() is None


def test_limit_and_offset(models):
    _, Users, Posts = models
    pairs = Posts.join(Users).order_by("id").limit(1).offset(1).fetch()
    assert [p.id for p, _ in pairs] == ["p2"]


def test_no_foreign_key_requires_explicit_on(models):
    database, Users, _ = models

    @model(database)
    class Tags(BaseModel):
        __schema__ = (Primary("id"),)
        id: str
        owner: str

    with raises(ValueError):
        Users.join(Tags)
    assert Users.join(Tags, on=("id", "owner")).fetch() == []  # explicit is fine


def test_ambiguous_foreign_keys_require_explicit_on(models):
    database, Users, _ = models

    @model(database)
    class Messages(BaseModel):
        __schema__ = (
            Primary("id"),
            Foreign("sender", Users),
            Foreign("receiver", Users),
        )
        id: str
        sender: str
        receiver: str

    Messages.create(id="m1", sender="u1", receiver="u2")
    with raises(ValueError):
        Messages.join(Users)
    pairs = Messages.join(Users, on=("receiver", "id")).fetch()
    assert [u.username for _, u in pairs] == ["bob"]


def test_joining_a_model_with_itself_is_rejected(models):
    _, Users, _ = models
    with raises(ValueError):
        Users.join(Users, on=("id", "id"))
