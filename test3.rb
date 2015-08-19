#!/apollo/bin/env -e SXHydScripts ruby

require 'getoptlong'
require 'optparse'
require 'date'
require 'amazon/config/appconfig'
require 'rubygems'
require 'amazon/bsf'
require 'pp'
require 'logger'
require 'csv'
require 'time'
require 'mysql2'
#require 'datetime'
require File.expand_path('../utils', __FILE__)

# GLOBAL VARS
$error = 0
$verbose = false

# Parse options
opts = GetoptLong.new(
    [ "--verbose", "-o", GetoptLong::NO_ARGUMENT ],
    [ "--domain",   "-d", GetoptLong::REQUIRED_ARGUMENT ],
    [ "--date", GetoptLong::REQUIRED_ARGUMENT ],
    [ "--realm",   "-r", GetoptLong::REQUIRED_ARGUMENT ],
)

opts.each do | opt, arg |
   $domain  = arg.to_s if opt == '--domain'
   $realm  = arg.to_s if opt == '--realm'
   $date  = arg.to_s if opt == '--date'
   $verbose = true if opt == '--verbose'
end

if ( !$domain || !$realm || !$date)
    print "Usage: \n"
    print "  --domain                                       Domain for connecting to rds host\n"
    print "  --realm                                        Realm to connect to rds host\n"
    print "  --date                                         Run date for the job\n"
    print "  --verbose                                      Debug Mode\n"
    exit
end

# Set up standard config
$appconfig = Amazon::Config::AppConfig.new(:override => 'SXHydScripts.cfg', :domain=>$domain, :realm=>$realm)

# CONSTANTS
logfilename = "/tmp/wmsr_inventory_data.#{Time.now.to_i}.log"
loghandle = File.open(logfilename, 'w')
log = Logger.new(loghandle)
Logger.new(STDOUT).info("Log written to '#{logfilename}'")
SOURCE_DIR = "/home/metron/wmsrData"
MAX_BATCH_SIZE = 1000
DAYS_BEFORE = 1
DELETE_FILE_DAYS = 7

#Get the file name per realm
$free_form = $appconfig["RealmConfigsReverseMapping"][$realm]
$free_form = "FE" if $free_form == "CN"
$FILE_NAME = SOURCE_DIR + "/" + "WMSR_First_Last_Receive_" + $date + "_" + $free_form + ".csv"
#temp filename


$FILE_NAME = SOURCE_DIR + "/" + "WMSR_Inventory_test_Data_2015-07-31_NA.csv"

$FILE_NAME = SOURCE_DIR + "/" + "inventory_backfill_test_31_1000000.csv"


pp "Processing file #{$FILE_NAME}..."

$current_batch_size = 0
$inventory_data_rows = Array.new
$batch_count = 0
$record_count = 0
$inserted_record_count = 0
$racerxDb = connect_to_wmsr_db($appconfig)

def create_batch_query
    query = ""
    $first_flag = 0
    $inventory_data_rows.each do |r|
        if $first_flag == 0
            query = "('#{r['region_id']}','#{r['received_date']}','#{r['warehouse']}','#{r['metric_id']}','#{r['asin']}','#{r['quantity']}','#{r['bin_id']}','#{r['iog']}','#{r['order_id']}','#{r['dw_last_updated']}')"
            $first_flag = 1
        else
            query = query + ",('#{r['region_id']}','#{r['received_date']}','#{r['warehouse']}','#{r['metric_id']}','#{r['asin']}','#{r['quantity']}','#{r['bin_id']}','#{r['iog']}','#{r['order_id']}','#{r['dw_last_updated']}')"
        end
    end
    return query
end

def export_to_rds
   # pp "INSERT INTO wmsr_inventory_data(fnsku,asin,snapshot_day,receive_day,warehouse,original_quantity, transit_quantity,region_id,marketplace_id,merchant_id,po_id,shipment_id) VALUES#{create_batch_query} ON DUPLICATE KEY UPDATE original_quantity=VALUES(original_quantity), transit_quantity=VALUES(transit_quantity), shipment_id=VALUES(shipment_id)" if $verbose
    $racerxDb.query "INSERT INTO wmsr_inventory_data(region_id,received_date,warehouse,metric_id,asin,quantity,bin_id,iog,order_id,dw_last_updated) VALUES#{create_batch_query}"
    $inserted_record_count = $inserted_record_count + $racerxDb.affected_rows
end

$start_time = Time.now

$error_count = 0

File.foreach($FILE_NAME) do |row|
    $record_count = $record_count + 1
    row.encode!('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
    row = row.gsub('"',"&quot;")
    row = row.gsub("\n",'')

    if row == "REGION_ID    RECEIVED_DATE   WAREHOUSE   METRIC_ID   ASIN    QUANTITY    RECEIVED_DATETIME   BIN_ID  IOG ORDER_ID    DW_LAST_UPDATED"
        next
    end

    if row == "REGION_ID\tRECEIVED_DATE\tWAREHOUSE\tMETRIC_ID\tASIN\tQUANTITY\tRECEIVED_DATETIME\tBIN_ID\tIOG\tORDER_ID\tDW_LAST_UPDATED"
        next
    end

   # pp "row '#{row}'"

    $tab_split_row = row.split(/\t/)

    $inventory_data_row = {}

    #pp "tab split row: "
    #pp $tab_split_row

    if $tab_split_row.length == 11
	    $inventory_data_row["region_id"] = $tab_split_row[0]
        $inventory_data_row["received_date"] = Time.parse($tab_split_row[1]).strftime('%Y-%m-%d %T')
        $inventory_data_row["warehouse"] = $tab_split_row[2]
        $inventory_data_row["metric_id"] = $tab_split_row[3]
        $inventory_data_row["asin"] = $tab_split_row[4]
        $inventory_data_row["quantity"] = $tab_split_row[5]
        $inventory_data_row["received_datetime"] = Time.parse($tab_split_row[6]).strftime('%Y-%m-%d %T')
        $inventory_data_row["bin_id"] = $tab_split_row[7]
        $inventory_data_row["iog"] = $tab_split_row[8]
        $inventory_data_row["order_id"] = $tab_split_row[9]
        $inventory_data_row["dw_last_updated"] = Time.parse($tab_split_row[10]).strftime('%Y-%m-%d %T')
    else
        $error_count += 1
        #pp "Record is less than required length. row: #{row}. Record count: #{$record_count}. Error count: #{$error_count}" if $verbose
        #log.info("Record is less than required length. row: #{row}. Record count: #{$record_count}. Error count: #{$error_count}")
    end

    $inventory_data_rows.push($inventory_data_row)

    $current_batch_size = $current_batch_size + 1

    begin
        if $current_batch_size == MAX_BATCH_SIZE
            $current_batch_size = 0
            if $batch_count % 100 == 0
	    	log.info("current batch count: #{$batch_count}")
            #break
	#	pp "batch count: #{$batch_count}"
	    end
            export_to_rds
      	    #pp "batch: #{$batch_count}" if $verbose
            #reset the batch
            $batch_count = $batch_count + 1
            $inventory_data_rows = Array.new
        end
    rescue => e
        $inventory_data_rows = Array.new # In worst case do not export it
        log.error("#{e.class}: #{e.message}\n#{e.backtrace.join("\n")}")
        pp "[ERROR] #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}" if $verbose
        next
    end
end

begin
    if $inventory_data_rows.length > 0
        export_to_rds
    end
rescue => e
    log.error("#{e.class}: #{e.message}\n#{e.backtrace.join("\n")}")
    #pp "#{e.class}: #{e.message}\n#{e.backtrace.join("\n")}" if $verbose
end

$end_time = Time.now
$time_taken = $end_time - $start_time

pp "Total time taken = #{$time_taken}" if $verbose
pp "Total records processed = #{$record_count}" if $verbose
pp "Total batches = #{$batch_count}" if $verbose
pp "Total records inserted = #{$inserted_record_count}" if $verbose
pp "Error count: #{$error_count}" if $verbose


log.info("Total time taken = #{$time_taken}")
log.info("Total records processed = #{$record_count}" )
log.info("Total batches = #{$batch_count}")
log.info("Total records inserted = #{$inserted_record_count}")
log.info("Error count: #{$error_count}")

#put deleting files code here
#for i in DELETE_FILE_DAYS..(DELETE_FILE_DAYS+7)
#    $weekBefore = Date.today - i
#    $delete_date = $weekBefore.strftime('%Y-%m-%d')
#    delete_file_name = SOURCE_DIR + "/" + "File_name_" + $delete_date + "_1" + ".txt"
#    if File.exists(delete_file_name)
#        pp "Deleteing file #{delete_file_name}..." if $verbose
#        File.delete(delete_file_name)
#    end
#end