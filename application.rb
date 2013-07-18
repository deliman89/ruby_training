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
    client.query "BEGIN"
    CSV.foreach(file, :headers => :first_row) { |record|
      p record
      fname = record[0]
      lname = record[1]
      dob = FormatDate.new(record[2]).newMethod
      age = record[3]
      client.query "Insert into Customers Values('#{fname}', '#{lname}', '#{age}', '#{dob}', '#{Time.now.strftime("%Y-%m-%d %H:%M:%S")}')"
    }
  }
  client.query "Insert into Logs values('Customers Load', '', 'Success', '#{Time.now.strftime("%Y-%m-%d %H:%M:%S")}')"
  files.each{|file| FileUtils.mv(file, "backups/")}
rescue Exception => e
  $message = e.message
  client.query "ROLLBACK"
  client.query "Insert into Logs values('Customers Load', '#{$message.gsub("'","''")}', 'Failed', '#{Time.now.strftime("%Y-%m-%d %H:%M:%S")}')"
ensure
  client.query "COMMIT"    
end

