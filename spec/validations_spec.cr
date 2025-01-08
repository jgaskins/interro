require "./spec_helper"

require "../src/validations"

module ValidationsSpec
  include Interro::Validations
  extend self

  describe Interro::Validations do
    describe "#validate_presence" do
      it "validates presence" do
        result(&.validate_presence(string: "hello", string2: "hello")).should eq true
      end

      it "returns failure for an empty string" do
        result = result(&.validate_presence(string: "")).as(Failure)

        result.errors.should eq ["string must not be blank"]
      end

      it "returns failure for a nil value" do
        result = result(&.validate_presence(string: nil)).as(Failure)

        result.errors.should eq ["string must not be blank"]
      end

      it "returns failure for multiple validation errors" do
        result = result(&.validate_presence(string: nil, string2: "")).as(Failure)

        result.errors.should eq [
          "string must not be blank",
          "string2 must not be blank",
        ]
      end
    end

    describe "#validate_format_test" do
      it "validates format" do
        result(&.validate_format(/foo/, string: "food")).should eq true
      end

      it "returns failure when the string does not match the pattern" do
        result = result(&.validate_format(/foo/, string: "nope")).as(Failure)

        result.errors.should eq ["string is in the wrong format"]
      end

      it "returns a custom failure message" do
        result = result(&.validate_format("string", "nope", /foo/, failure_message: "must contain 'foo'")).as(Failure)

        result.errors.should eq ["string must contain 'foo'"]
      end

      it "returns a custom failure message without automatically injecting the name" do
        result = result(&.validate_format("nope", /foo/, failure_message: "string must contain 'foo'")).as(Failure)

        result.errors.should eq ["string must contain 'foo'"]
      end
    end

    describe "#validate_size" do
      it "validates the size of a string" do
        result(&.validate_size("my_string", "123", 3..3, "characters")).should eq true
      end

      it "validates the size of an array" do
        result(&.validate_size("my_list", [1, 2, 3], 3..3, "numbers")).should eq true
      end

      it "validates size with infinite top-end" do
        result(&.validate_size("my_string", "123", 3.., "characters")).should eq true
        result(&.validate_size("my_string", "123", 2.., "characters")).should eq true
      end

      it "validates size with infinite bottom end" do
        string = "1234567890"
        10.times do |size|
          result(&.validate_size("my_string", string, size.., "characters")).should eq true
        end
      end

      it "validates size with exclusive top end" do
        result(&.validate_size("my_string", "1234567890", 1...11, "characters")).should eq true

        result(&.validate_size("my_string", "1234567890", 1...10, "characters"))
          .as(Failure)
          .errors
          .should eq ["my_string must be 1-9 characters"]
      end

      it "returns failure when the size of the value is outside the range" do
        result = result(&.validate_size("my_string", "123", 1..2, "characters")).as(Failure)

        result.errors.should eq ["my_string must be 1-2 characters"]
      end

      it "returns failure when the size of an array is outside the range" do
        result = result(&.validate_size("my_list", [1, 2, 3, 4], 1..2, "numbers")).as(Failure)

        result.errors.should eq ["my_list must be 1-2 numbers"]
      end

      it "returns failure when the size of the value is above the range when it is infinite on the low end" do
        result = result(&.validate_size("my_string", "123", ..2, "characters")).as(Failure)

        result.errors.should eq ["my_string must be at most 2 characters"]
      end

      it "returns failure when the size of the value is above the range when it is infinite+exclusive on the low end" do
        result = result(&.validate_size("my_string", "123", ...3, "characters")).as(Failure)

        result.errors.should eq ["my_string must be at most 2 characters"]
      end

      it "returns failure when the size of the value is below the range when it is infinite on the high end" do
        result = result(&.validate_size("my_string", "123", 4.., "characters")).as(Failure)

        result.errors.should eq ["my_string must be at least 4 characters"]
      end

      it "returns failure when the size of the value is below the range when it is infinite+exclusive on the high end" do
        result = result(&.validate_size("my_string", "123", 4..., "characters")).as(Failure)

        result.errors.should eq ["my_string must be at least 4 characters"]
      end
    end

    describe "#validate_uniqueness" do
      emails = %w[me@example.com you@example.com]

      it "validates the uniqueness by using a block to check for existing values" do
        result(&.validate_uniqueness("email") { emails.includes? "unique@example.com" }).should eq true
      end

      it "validates the uniqueness by using a block to check for existing values" do
        result = result(&.validate_uniqueness("email") { emails.includes? "me@example.com" }).as(Failure)

        result.errors.should eq ["email has already been taken"]
      end

      it "validates the uniqueness by using a block to check for existing values" do
        result = result(&.validate_uniqueness(message: "Email must be unique") { emails.includes? "me@example.com" })
          .as(Failure)

        result.errors.should eq ["Email must be unique"]
      end
    end

    describe "#validate" do
      it "validates that a block returns truthy" do
        result(&.validate("this should pass") { "passing" }).should eq true
      end

      it "returns failure with the given error message when the block is falsy" do
        result = result(&.validate("D'oh!") { nil }).as(Failure)

        result.errors.should eq ["D'oh!"]
      end
    end
  end

  def result
    result = Interro::Validations::Result(Bool).new
    yield(result).valid { true }
  end
end
