require "uuid"

module Interro
  alias Primitive = Nil |
                    String |
                    Int8 | Int16 | Int32 | Int64 |
                    Time |
                    UUID

  alias Value = Primitive | Array(Primitive)
end
