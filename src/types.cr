require "uuid"

module Interro
  alias Value = Nil |
                Bool |
                String |
                Int8 | Int16 | Int32 | Int64 |
                Time |
                UUID |
                Array(Value)
end
