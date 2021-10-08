require "./spec_helper"

require "../src/interro"

pg = DB.open("postgres:///")
pg.exec %{CREATE EXTENSION IF NOT EXISTS "uuid-ossp"}
pg.exec "DROP TABLE IF EXISTS users"
pg.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    deactivated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL
pg.exec "CREATE INDEX IF NOT EXISTS index_users_on_email ON users (email)"
pg.exec "CREATE INDEX IF NOT EXISTS index_users_on_created_at ON users (created_at)"

pg.exec "DROP TABLE IF EXISTS groups"
pg.exec <<-SQL
  CREATE TABLE groups (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    member_count INT8 NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL

pg.exec "DROP TABLE IF EXISTS group_memberships"
pg.exec <<-SQL
  CREATE TABLE group_memberships (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    group_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_group_id ON group_memberships (group_id)"
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_user_id ON group_memberships (user_id)"
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_created_at ON group_memberships (created_at)"

private def create_user(email = "user-#{UUID.random}@example.com", name = "Another User") : User
  UserQuery.new.create(email: email, name: name)
end

private def create_group(name = "Group #{UUID.random}")
  GroupQuery.new.create(name: name)
end

Interro.config do |c|
  c.db = pg
  # equivalent to:
  #   c.read_db = pg
  #   c.write_db = pg

  # c.log.level = Log::Severity::Debug
end

struct User
  include DB::Serializable

  getter id : UUID
  getter email : String
  getter name : String
  getter deactivated_at : Time?
  getter created_at : Time
  getter updated_at : Time

  @[DB::Field(ignore: true)]
  property groups : Array(Group)?
end

struct Group
  include DB::Serializable

  getter id : UUID
  getter name : String
  getter? active : Bool
  getter member_count : Int64
  getter created_at : Time
  getter updated_at : Time
end

struct GroupMembership
  include DB::Serializable

  @user_id : UUID
  @group_id : UUID

  getter id : UUID
  getter created_at : Time

  def user
    @user ||= UserQuery.new.with_id(@user_id).first
  end

  def group
    @group ||= GroupQuery.new.with_id(@group_id).first
  end
end

struct FakeUser
  include Interro::Model

  @[Interro::Field(select: "uuid_generate_v4()", as: "id")]
  getter id : UUID

  @[Interro::Field(select: "md5(random()::text)")]
  getter name : String
end

struct UserQuery < Interro::QueryBuilder(User)
  table "users"

  def create(email : String, name : String)
    insert email: email, name: name
  end

  def destroy(user : User)
    self
      .where(id: user.id)
      .delete
  end

  def find!(**attrs)
    find(**attrs) || raise Interro::NotFound.new("Cannot find a #{T} with attributes #{attrs.inspect}")
  end

  def with_id(id : UUID)
    where id: id
  end

  def with_name(name : String)
    where name: name
  end

  def registered_after(time : Time)
    where { |user| user.created_at > time }
  end

  def registered_before(time : Time)
    where { |user| user.created_at < time }
  end

  def registered_before_with_multiple_where_clauses(time : Time)
    where { |user| user.created_at < time }
      .where { |user| user.updated_at < time }
  end

  def registered_before_with_compound_where_clause(time : Time)
    where { |user| (user.created_at < time) & (user.updated_at < time) }
  end

  def in_reverse_chronological_order
    order_by(created_at: "desc")
  end

  def at_most(count : Int32)
    limit count
  end

  def change_name(user : User, name : String)
    where(id: user.id)
      .update(name: name)
  end

  def with_id_in_kwargs(ids : Array(UUID))
    where id: ids.map(&.as(Interro::Value))
  end

  def with_id_in_block(ids : Array(UUID))
    where { |user| user.id.in? ids }
  end

  def with_id_and_name(id : UUID, name : String)
    where id: id, name: name
  end

  def delete_with_id_and_name(id : UUID, name : String)
    with_id_and_name(id, name).delete
  end

  def members_of_group(group : Group)
    members_of_group_with_id group.id
  end

  def active
    where deactivated_at: nil
  end

  def active_but_with_a_block
    where { |user| user.deactivated_at == nil }
  end

  def members_of_group_with_id(id : UUID)
    self
      .inner_join("group_memberships", as: "gm", on: "gm.user_id = users.id")
      .where("gm.group_id": id)
  end

  def deactivate!(user : User)
    with_id(user.id).update deactivated_at: Time.utc
  end

  def search(term : String)
    where("name", "@@", "$1", [term])
  end

  def count : Int64
    scalar("count(*)", as: Int64)
  end

  def lock_rows
    for_update
  end
end

struct GroupQuery < Interro::QueryBuilder(Group)
  table "groups"

  def create(name : String)
    insert name: name
  end

  def change_name(group : Group, name : String)
    where(id: group.id)
      .update(name: name)
  end

  def with_id(id : UUID) : self
    where id: id
  end

  def with_name(name : String) : self
    where "groups.name": name
  end

  def with_member(user)
    join_to_users
      .where("users.id": user.id)
  end

  def with_members
    columns = String.build do |str|
      select_columns str
      str << ", "
      UserQuery.new.select_columns str
    end

    join_to_users
      .fetch(columns, as: {Group, User})
  end

  def at_most(count : Int32)
    limit count
  end

  def in_alphanumeric_order
    order_by name: "ASC"
  end

  def increment_member_count(group : Group)
    where(id: group.id).update "member_count = member_count + 1"
  end

  private def join_to_users
    self
      .inner_join("group_memberships", as: "gm", on: "gm.group_id = groups.id")
      .inner_join("users", on: "gm.user_id = users.id")
  end
end

struct GroupMembershipQuery < Interro::QueryBuilder(GroupMembership)
  table "group_memberships"

  def create(user : User, group : Group) : GroupMembership
    Interro.transaction do |txn|
      GroupQuery[txn].increment_member_count group
      with_transaction(txn).insert user_id: user.id, group_id: group.id
    end
  end
end

struct FakeUserQuery < Interro::QueryBuilder(FakeUser)
  table "generate_series(1, 1000)", as: "fake_users"
end

describe Interro do
  it "can create a row" do
    email = "foo-#{UUID.random}@example.com"

    user = UserQuery.new.create(email: email, name: "Foo")

    user.should be_a User
    user.email.should eq email
    user.name.should eq "Foo"
  end

  it "can find a row" do
    email = "finduser-#{UUID.random}@example.com"
    created_user = UserQuery.new.create(email: email, name: "Find User")

    found_user = UserQuery.new.find!(email: email)

    found_user.email.should eq email
    found_user.name.should eq "Find User"
    found_user.id.should eq created_user.id
    found_user.created_at.should eq created_user.created_at
    found_user.updated_at.should eq created_user.updated_at
  end

  it "can handles comparisons with NULL" do
    query = UserQuery.new
    active = query.create name: "Active", email: "active-#{UUID.random}@example.com"
    inactive = query.create name: "Inactive", email: "inactive-#{UUID.random}@example.com"
    query.deactivate! inactive

    active_users = query.active
    active_via_block = query.active_but_with_a_block

    active_users.should contain active
    active_users.should_not contain inactive
    active_via_block.to_a.should eq active_users.to_a
  end

  describe Interro::QueryBuilder do
    query = UserQuery.new
    created_users = Array.new(11) { query.create email: UUID.random.to_s, name: UUID.random.to_s }

    it "can build a query" do
      users = query
        .registered_before(created_users[7].created_at)
        .in_reverse_chronological_order
        .at_most(5)
        .to_a

      users.size.should eq 5
      users.should eq created_users[2...7].reverse
    end

    it "can build a query with a compound where clause" do
      users = query
        .registered_before_with_compound_where_clause(created_users[7].created_at)
        .in_reverse_chronological_order
        .at_most(5)
        .to_a

      users.size.should eq 5
      users.should eq created_users[2...7].reverse
    end

    it "can build a query with multiple where clauses" do
      users = query
        .registered_before_with_multiple_where_clauses(created_users[7].created_at)
        .in_reverse_chronological_order
        .at_most(5)
        .to_a

      users.size.should eq 5
      users.should eq created_users[2...7].reverse
    end

    it "can build a query and get the first result" do
      user = query
        .registered_before_with_multiple_where_clauses(created_users[7].created_at)
        .in_reverse_chronological_order
        .first?

      user.should eq created_users[6]
    end

    it "can build a query with keyword args" do
      user = query
        .with_id(created_users[0].id)
        .with_name(created_users[0].name)
        .in_reverse_chronological_order
        .first?

      user.should eq created_users[0]
    end

    it "can return an iterator" do
      users = query
        .registered_after(created_users[7].created_at)
        .registered_before(created_users[10].created_at)
        .in_reverse_chronological_order
        .each
        .map(&.itself)
        .to_a

      users.size.should eq 2
      users.should eq created_users[8..9].reverse
    end

    it "can be used to return all values" do
      current_size = GroupQuery.new.size

      group = GroupQuery.new.create name: "First group"

      GroupQuery.new.size.should eq current_size + 1
      GroupQuery.new.should contain group
    end

    it "can join tables" do
      user = created_users[0]
      group = GroupQuery.new.create(name: "My Group")
      GroupMembershipQuery.new.create(user: user, group: group)

      groups = GroupQuery.new
        .with_member(user)
        .to_a

      groups.size.should eq 1
      groups.map(&.id).should contain group.id
    end

    describe "matching values in an array" do
      ids = created_users.map(&.id).first(3)

      it "can find a record with a value in an array" do
        users = query
          .with_id_in_block(ids)
          .to_a

        users.should contain created_users[0]
        users.should contain created_users[1]
        users.should contain created_users[2]

        users.should_not contain created_users[3]
      end

      it "can find records using keyword args" do
        users = query
          .with_id_in_kwargs(ids)

        users = users
          .to_a

        users.should contain created_users[0]
        users.should contain created_users[1]
        users.should contain created_users[2]

        users.should_not contain created_users[3]
      end

      it "can find records using multiple keyword args" do
        users = query
          .with_id_and_name(id: created_users[0].id, name: created_users[0].name)

        users.size.should eq 1
        users.should contain created_users[0]
      end

      it "can delete records using multiple keyword args" do
        user = created_users[0]

        query.delete_with_id_and_name(id: user.id, name: user.name)

        users = query.with_id_and_name(id: user.id, name: user.name)

        users.size.should eq 0
      end
    end

    it "can return scalar values" do
      query.count.should be_a Int64
    end

    it "can update records" do
      users = query.change_name(created_users[1], name: "Jamie")
      users.size.should eq 1

      user = users.first
      users.first.id.should eq created_users[1].id
      users.first.name.should eq "Jamie"
    end

    it "can update records with SQL expressions" do
      group = create_group

      results = GroupQuery.new.increment_member_count(group).to_a
      results.size.should eq 1
      group = results.first

      group.member_count.should eq 1
    end

    it "can delete records" do
      deleted = query.create(email: "deleteme+#{UUID.random}@example.com", name: "Delete Me")
      not_deleted = query.create(email: "dontdeleteme+#{UUID.random}@example.com", name: "Don't Delete Me")

      # Nothing up my sleeves
      query.with_id(deleted.id).should contain deleted
      query.with_id(not_deleted.id).should contain not_deleted

      query.destroy deleted

      query.with_id(deleted.id).should be_empty
      query.with_id(not_deleted.id).should_not be_empty
      query.with_id(not_deleted.id).should contain not_deleted
    end

    it "can run UNION queries" do
      lhs = Array.new(3) { create_user(name: "LHS") }
      rhs = Array.new(3) { create_user(name: "RHS") }
      excluded = create_user(name: "excluded")
      lhs_users = query.with_name("LHS")
      rhs_users = query.with_name("RHS")

      users = (lhs_users | rhs_users).to_a

      lhs.all? { |user| users.includes? user }.should eq true
      rhs.all? { |user| users.includes? user }.should eq true
      users.should_not contain excluded
    end

    it "can run subqueries" do
      included = create_user(email: "included")
      excluded = create_user(email: "excluded")
      group = create_group
      another_group = create_group
      GroupMembershipQuery.new.create(user: included, group: group)
      GroupMembershipQuery.new.create(user: excluded, group: another_group)

      users = query.members_of_group_with_id(group.id).to_a

      users.should contain included
      users.should_not contain excluded
    end

    it "can use arbitrary operators" do
      user = create_user(name: "Search User")

      UserQuery.new.search("search").should contain user
    end

    describe "transactions" do
      it "commits without error" do
        user = nil
        group = nil

        Interro.transaction do |txn|
          user = UserQuery[txn].create(name: "Transaction User", email: "user-#{UUID.random}@example.com")
          group = GroupQuery[txn].create(name: "Transaction Group")
        end

        if user && group
          UserQuery.new.with_id(user.id).first.should be_a User
          GroupQuery.new.with_id(group.id).first.should be_a Group
        else
          raise "One of these are nil and should not be: #{{user: user, group: group}}"
        end
      end

      it "rolls back if there is an error" do
        user = nil
        group = nil

        Interro.transaction do |txn|
          user = UserQuery[txn].create(name: "Transaction User", email: "user-#{UUID.random}@example.com")
          group = GroupQuery[txn].create(name: "Transaction Group")
          raise "hell"
        end
      rescue ex
        raise ex if ex.message != "hell" # Make sure we're rescuing the right exception
      ensure

        if user && group
          UserQuery.new.with_id(user.id).first?.should eq nil
          GroupQuery.new.with_id(group.id).first?.should eq nil
        else
          raise "One of these are nil and should not be: #{{user: user, group: group}}"
        end
      end

      # This spec may seem silly, but transactions are isolated (the I in ACID)
      # so we need to be sure that reads happen within that bubble.
      it "reads its own writes" do
        group = GroupQuery.new.create name: "The Group"

        Interro.transaction do |txn|
          gq = GroupQuery[txn]

          gq.change_name(group, "The Same Group")
          gq.with_id(group.id).first.name.should eq "The Same Group"
        end
      end
    end

    describe "returning different types" do
      it "returns a tuple with objects of the given types" do
        group = create_group
        user = create_user
        GroupMembershipQuery.new.create(group: group, user: user)

        group_with_members = GroupQuery.new
          .with_members
          .map { |(g, u)| {g.id, u.id} }

        group_with_members.to_a.should contain({group.id, user.id})
      end
    end

    # I'm not 100% sure if the implementation of this test is a great idea
    it "can lock records with FOR UPDATE" do
      user = create_user
      Interro.transaction do |txn|
        fetched_user = UserQuery[txn]
          .with_id(user.id)
          .lock_rows
          .to_sql
          .should end_with "FOR UPDATE"
      end
    end

    it "can select SQL expressions instead of just columns" do
      fake_user = FakeUserQuery.new.first

      fake_user.id.should be_a UUID
      fake_user.name.bytesize.should eq 32 # MD5 hexdigests are 32 bytes
    end
  end
end

struct CreateUser < Interro::Query
  def call(email : String, name : String) : User
    write_one <<-SQL, email, name, as: User
      INSERT INTO users (email, name)
      VALUES ($1, $2)
      RETURNING *
    SQL
  end
end

struct UpdateUserName < Interro::Query
  def call(user : User, name : String) : User
    write_one <<-SQL, user.id, name, as: User
      UPDATE users
      SET name = $2
      WHERE id = $1
      RETURNING *
    SQL
  end
end

struct CreateGroup < Interro::Query
  def call(name : String) : Group
    write_one <<-SQL, name, as: Group
      INSERT INTO groups (name)
      VALUES ($1)
      RETURNING *
    SQL
  end
end

struct AddUserToGroup < Interro::Query
  def call(user : User, group : Group)
    write <<-SQL, user.id, group.id
      INSERT INTO group_memberships (user_id, group_id)
      VALUES ($1, $2)
    SQL
  end
end

struct AnyGroupsWithName < Interro::Query
  # Yes, this is a terribly contrived example, but we need something easy to
  # understand that exercises read_each.
  def call(name : String)
    sql = <<-SQL
      SELECT name
      FROM groups
    SQL

    read_each sql, as: {String} do |(group_name)|
      if group_name == name
        return true
      end
    end

    false
  end
end

struct GetUserWithGroups < Interro::Query
  def call(id : UUID) : User
    user = UserQuery.new
      .with_id(id)
      .first

    user.groups = read_all(<<-SQL, id, as: Group)
      SELECT DISTINCT g.*
      FROM groups g
      JOIN group_memberships m ON m.group_id = g.id
      WHERE m.user_id = $1
    SQL

    user
  end
end

describe Interro::Query do
  it "lets you subclass to write specialized queries" do
    user = CreateUser[email: "#{UUID.random}@example.com", name: UUID.random.to_s]

    updated_user = UpdateUserName[user, name: "Jamie"]

    updated_user.id.should eq user.id
    updated_user.name.should eq "Jamie"
  end

  it "can combine multiple DB operations into a cohesive query object" do
    user = CreateUser[email: "#{UUID.random}@example.com", name: "Jamie"]
    members = CreateGroup["Members"]
    admins = CreateGroup["Admins"]
    banned = CreateGroup["Banned"]
    AddUserToGroup[user, members]
    AddUserToGroup[user, admins]

    selected = GetUserWithGroups[user.id]

    if groups = selected.groups
      groups.should contain members
      groups.should contain admins
      groups.should_not contain banned
    else
      raise "GROUPS WERE NOT SET"
    end
  end

  it "can take a QueryBuilder do use its transaction" do
    Interro.transaction do |txn|
      CreateUser.new(UserQuery[txn]).@read_db.should eq txn.connection
    end

    CreateUser.new(UserQuery.new).@read_db.should eq Interro::CONFIG.read_db
  end

  it "can iterate over results as they're read from the DB" do
    group = CreateGroup[UUID.random.to_s]

    AnyGroupsWithName[group.name].should eq true
    AnyGroupsWithName[UUID.random.to_s].should eq false
  end
end
