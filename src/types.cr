require "uuid"

module Interro
  alias Value = Nil |
                Bool |
                String |
                Bytes |
                Int8 | Int16 | Int32 | Int64 |
                Float64 |
                Time |
                UUID |
                Array(Any)

  record Any, value : Value do
    def to_s(io)
      value.to_s io
    end

    def inspect(io)
      value.inspect io
    end
  end
end

module PQ
  struct Param
    def self.encode(any : Interro::Any)
      encode any.value
    end

    def self.encode_array(io, any : Interro::Any)
      encode_array io, any.value
    end
  end
end
