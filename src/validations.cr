module Interro
  # Validate inputs to your queries before saving the values to the database.
  # Since `QueryBuilder` includes `Validations`, it allows you to reference
  # `Result` and `Failure` by their shorthand names.
  #
  # ```
  # struct UserQuery < Interro::QueryBuilder(User)
  #   def create(*, name : String, email : String, team : Team, role : User::Role)
  #     Result(User).new
  #       .validate_presence(name: name, email: email)
  #       .validate_uniqueness("email") { where(email: email).any? }
  #       .valid do
  #         insert(
  #           name: name,
  #           email: email,
  #           team_id: team.id,
  #           role: role.value,
  #         )
  #       end
  #   end
  # end
  # ```
  module Validations
    # The entrypoint into validating your objects is to instantiate a
    # `Result(T)`, which you then call validation methods on. When you're
    # finished calling validations, you call `valid` with a block. That block
    # will execute if your values pass all validations, returning the value
    # in the block (which must be the `T` type of the `Result`), or an
    # `Interro::Validations::Failure` object, which contains the validation
    # errors for the given inputs.
    #
    # ```
    # struct UserQuery < Interro::QueryBuilder(User)
    #   def create(*, name : String, email : String, team : Team, role : User::Role)
    #     Result(User).new
    #       .validate_presence(name: name, email: email)
    #       .validate_uniqueness("email") { where(email: email).any? }
    #       .validate_format(/\w@\w/, email: email, failure_message: "must be a valid email address")
    #       .valid do
    #         insert(
    #           name: name,
    #           email: email,
    #           team_id: team.id,
    #           role: role.value,
    #         )
    #       end
    #   end
    # end
    # ```
    struct Result(T)
      protected getter errors : Array(Error) { [] of Error }

      # Validate whether all of the `values` are present by calling `.presence`
      # on them.
      #
      # ```
      # Result(Post).new
      #   .validate_presence(title: title, body: body)
      # ```
      def validate_presence(**values) : self
        values.each do |name, value|
          validate name.to_s, "must not be blank" { value.presence }
        end

        self
      end

      # Validate that all of the attributes match the expected format in the
      # `Regex`.
      #
      # ```
      # Result(User).new
      #   .validate_format(/\w@\w/, email: email)
      # ```
      def validate_format(format : Regex, **attributes) : self
        attributes.each do |attr, value|
          validate_format attr.to_s, value, format, failure_message: "is in the wrong format"
        end

        self
      end

      # Validate that `value` matches the expected format in the `Regex`. This
      # method does not infer a name, so a custom `failure_message` must be
      # provided.
      #
      # ```
      # Result(User).new
      #   .validate_format(email, /\w@\w/, failure_message: "Email must be a valid email address (user@domain.tld)")
      # ```
      def validate_format(value : String, format : Regex, *, failure_message : String) : self
        validate_format nil, value, format, failure_message: failure_message
      end

      # Validate that `value` matches the expected format in the `Regex`, using
      # the attribute name specified in `name` and a default `failure_message`.
      #
      # ```
      # Result(User).new
      #   .validate_format("email", email, /\w@\w/, failure_message: "must be a valid email address")
      # ```
      def validate_format(name, value : String, format : Regex, *, failure_message : String = "is in the wrong format") : self
        validate name.to_s, failure_message do
          value =~ format
        end
      end

      # Validate that `value` is of the expected `size` range. Value can be any object that responds to `#size`.
      #
      # ```
      # Result(User).new
      #   .validate_size("username", username, 2..64, "characters")
      # ```
      def validate_size(name : String, value, size : Range, unit : String, *, failure_message = default_validate_size_failure_message(size, unit)) : self
        validate name, failure_message do
          size.includes? value.size
        end
      end

      private def default_validate_size_failure_message(size : Range, unit : String)
        case size
        when .finite?
          # Using `Range#min` and `Range#max` requires that neither be nil, and
          # `.finite?` doesn't guarantee that in a way the compiler recognizes,
          # so we need to do it with local variables
          if (min = size.begin) && (max = size.end)
            r = Range.new(min, max, size.excludes_end?)
            range = "#{r.min}-#{r.max}"
          else
            raise "bug"
          end
        when .begin
          if min = size.begin
            range = "at least #{min}"
          else
            raise "bug"
          end
        when .end
          if (max = size.end)
            # We have to account for ...max
            r = Range.new(max - 1, max, size.excludes_end?)
            range = "at most #{r.max}"
          else
            raise "bug"
          end
        end
        failure_message = "must be #{range} #{unit}"
      end

      # Validate that the given attribute is unique by executing a block that
      # returns truthy if any other objects exist with that attribute.
      #
      # ```
      # Result(User).new
      #   .validate_uniqueness("email") { where(email: email).any? }
      # ```
      def validate_uniqueness(attribute, &) : self
        validate attribute, "has already been taken" do
          !yield
        end
      end

      # Validate that the given attribute is unique by executing a block that
      # returns truthy if any other objects exist with that attribute.
      #
      # ```
      # Result(User).new
      #   .validate_uniqueness(message: "That email has already been taken") { where(email: email).any? }
      # ```
      def validate_uniqueness(*, message : String, &) : self
        validate "", message do
          !yield
        end
      end

      # Validate a block returns truthy, failing validation with the given `message` if it returns falsy.
      #
      # ```
      # struct Post < Interro::QueryBuilder(T)
      #   table "posts"
      #
      #   def create(*, title : String, body : String, by author : User, published : Bool, tags : Array(String)? = nil) : Post | Failure
      #     Result(Post).new
      #       .validate("published posts must have tags") do
      #         !published || tags.try(&.any?)
      #       end
      #       .valid do
      #         published_at = Time.utc if published
      #         insert(
      #           title: title,
      #           body: body,
      #           author_id: author.id,
      #           published_at: published_at,
      #           tags: tags,
      #         )
      #       end
      #   end
      # end
      # ```
      def validate(message : String, &)
        validate "", message do
          yield
        end
      end

      # Validate a block returns truthy, failing validation with the given `message` if it returns falsy.
      #
      # ```
      # struct Post < Interro::QueryBuilder(T)
      #   table "posts"
      #
      #   def create(*, title : String, body : String, by author : User, published : Bool, tags : Array(String)? = nil) : Post | Failure
      #     Result(Post).new
      #       .validate("tags", "must be populated for published posts") do
      #         !published || tags.try(&.any?)
      #       end
      #       .valid do
      #         published_at = Time.utc if published
      #         insert(
      #           title: title,
      #           body: body,
      #           author_id: author.id,
      #           published_at: published_at,
      #           tags: tags,
      #         )
      #       end
      #   end
      # end
      # ```
      def validate(attribute : String, message : String, &)
        unless yield
          errors << Error.new(attribute, message)
        end

        self
      end

      # Combine the errors of two different `Result` instances. The `T` types do not have to match.
      def |(other : Result)
        result = self.class.new
        result.errors = errors | other.errors
      end

      # Execute the block given if all validations have passed, otherwise return
      # a `Failure` containing all of the validation errors.
      def valid(&) : T | Failure
        if errors.empty?
          yield
        else
          Failure.new(errors.sort_by(&.attribute))
        end
      end
    end

    # Represents a validation error. Can be rendered directly to a template.
    record Error, attribute : String, message : String do
      def to_s(io : IO)
        unless attribute.empty?
          io << attribute << ' '
        end

        io << message
      end

      def ==(value : String)
        to_s == value
      end
    end

    # Returned when one or more validations did not pass and contains all of the validation errors.
    record Failure, errors : Array(Error) do
      def self.new(error_messages : Array(String)) : Failure
        new error_messages.map { |message| Error.new("", message) }
      end
    end
  end
end

# :nodoc:
struct Range
  def finite?
    !!(self.begin && self.end)
  end
end
