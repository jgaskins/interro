module Interro
  annotation Field
  end

  module Model
    include ::DB::Mappable

    macro included
      def self.new(rs : ::DB::ResultSet)
        instance = allocate
        instance.initialize(__set_for_db_serializable: rs)
        GC.add_finalizer(instance) if instance.responds_to?(:finalize)
        instance
      end

      def self.from_rs(rs : ::DB::ResultSet)
        objs = Array(self).new
        rs.each do
          objs << self.new(rs)
        end
        objs
      ensure
        rs.close
      end

      # Inject the class methods into subclasses as well

      macro inherited
        def self.new(rs : ::DB::ResultSet)
          super
        end

        def self.from_rs(rs : ::DB::Result_set)
          super
        end
      end
    end

    def initialize(*, __set_for_db_serializable rs : ::DB::ResultSet)
      {% begin %}
        {% properties = {} of Nil => Nil %}
        {% for ivar in @type.instance_vars %}
          {% ann = ivar.annotation(::Interro::Field) %}
          {% unless ann && ann[:ignore] %}
            {%
              properties[ivar.id] = {
                type:      ivar.type,
                key:       ((ann && ann[:key]) || ivar).id.stringify,
                default:   ivar.default_value,
                nilable:   ivar.type.nilable?,
                converter: ann && ann[:converter],
              }
            %}
          {% end %}
        {% end %}

        {% for name, value in properties %}
          %var{name} = nil
          %found{name} = false
        {% end %}

        rs.each_column do |col_name|
          case col_name
            {% for name, value in properties %}
              when {{value[:key]}}
                %found{name} = true
                %var{name} =
                  {% if value[:converter] %}
                    {{value[:converter]}}.from_rs(rs)
                  {% elsif value[:nilable] || value[:default] != nil %}
                    rs.read(::Union({{value[:type]}} | Nil))
                  {% else %}
                    rs.read({{value[:type]}})
                  {% end %}
            {% end %}
          else
            rs.read # Advance set, but discard result
            on_unknown_db_column(col_name)
          end
        end

        {% for key, value in properties %}
          {% unless value[:nilable] || value[:default] != nil %}
            if %var{key}.is_a?(Nil) && !%found{key}
              raise ::DB::MappingException.new("missing result set attribute: {{(value[:key] || key).id}}")
            end
          {% end %}
        {% end %}

        {% for key, value in properties %}
          {% if value[:nilable] %}
            {% if value[:default] != nil %}
              @{{key}} = %found{key} ? %var{key} : {{value[:default]}}
            {% else %}
              @{{key}} = %var{key}
            {% end %}
          {% elsif value[:default] != nil %}
            @{{key}} = %var{key}.is_a?(Nil) ? {{value[:default]}} : %var{key}
          {% else %}
            @{{key}} = %var{key}.as({{value[:type]}})
          {% end %}
        {% end %}
      {% end %}
    end

    protected def on_unknown_db_column(col_name)
      raise ::DB::MappingException.new("unknown result set attribute: #{col_name}")
    end

    module NonStrict
      protected def on_unknown_db_column(col_name)
      end
    end
  end
end
