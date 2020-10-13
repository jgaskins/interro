require "uuid"

module Interro
  alias Primitive = Nil |
                    Bool |
                    String |
                    Int8 | Int16 | Int32 | Int64 |
                    Time |
                    UUID

  alias Value = Primitive | Array(Primitive)
end
