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
    where id: ids.map(&.as(Interro::Primitive))
  end

  def with_id_in_block(ids : Array(UUID))
    where { |user| user.id.in? ids }
  end

  def count : Int64
    scalar("count(*)", as: Int64)
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

  def with_member(user)
    join_to_users
      .where("users.id": user.id)
  end

  def in_alphanumeric_order
    order_by name: "ASC"
  end

  private def join_to_users
    inner_join("group_memberships gm", on: "gm.group_id = groups.id")
      .inner_join("users", on: "gm.user_id = users.id")
  end
end

struct GroupMembershipQuery < Interro::QueryBuilder(GroupMembership)
  table "group_memberships"

  def create(user : User, group : Group) : GroupMembership
    insert user_id: user.id, group_id: group.id
  end
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

      groups.size.should eq 1
      groups.should contain group
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
    end

    it "can return scalar values" do
      query.count.should be_a Int64
    end

    it "can update records" do
      users = query.change_name(created_users[0], name: "Jamie")
      users.size.should eq 1

      user = users.first
      users.first.id.should eq created_users[0].id
      users.first.name.should eq "Jamie"
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
end
