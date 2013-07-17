require 'mysql2'
require 'yaml'
require 'fileutils'
require 'csv'
load 'functions/functions.rb'
config = YAML::load(open("#{File.expand_path(File.dirname(__FILE__))}/configs/configs.yml"))["mysql"]
client = Mysql2::Client.new(:host => config["host"], :username => config["username"], :database => config["database"])

files = "*.csv"
files = Dir.glob files
p files
client.query("Truncate table Customers")
begin
  files.each { |file|
  p file
    CSV.foreach(file, :headers => :first_row) { |record|
      p record
      fname = record[0]
      lname = record[1]
      dob = FormatDate.new(record[2]).newMethod
      #dob = Date.strptime(record[2], "%m/%d/%Y").strftime("%Y-%m-%d")
      age = record[3]
      #p "#{fname}, #{lname}, #{dob}, #{age}"
      client.query "Insert into Customers Values('#{fname}', '#{lname}', '#{age}', '#{dob}', '#{Time.now.strftime("%Y-%m-%d %H:%M:%S")}')"
    }
    # File.readlines("customers.txt").map { |records|
    #   records = records.gsub("\n", "")
    #   records = records.split(",")
    #   fname = records[0]
    #   lname = records[1]
    #   dob = Date.strptime(records[2], "%m/%d/%Y").strftime("%Y-%m-%d")
    #   age = records[3]
    #   client.query "Insert into Customers Values('#{fname}', '#{lname}', '#{age}', '#{dob}', '#{Time.now.strftime("%Y-%m-%d %H:%M:%S")}')"
    # }
  }
rescue Exception => e
  $message = e.message
  exit
end

# if $message == nil
  files.each{|file| FileUtils.mv(file, "backups/")}
# end
# alert = []
# alert << "Hello"

# p alert