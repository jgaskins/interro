require "pg"

# :nodoc:
class PG::ResultSet
  # We have to monkeypatch this to support the modification in DB::Serializable
  # above
  def each_column_from_last(&)
    (@column_index...column_count).each do |i|
      yield column_name(i)
    end
  end

  def read(type : DB::Serializable.class)
    type.new(self)
  end

  def read(type : Interro::Model.class)
    type.new(self)
  end
end
