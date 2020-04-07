require "./*"

module IPGeolocation
end

puts "loading index..."
lookup = IPGeolocation::Lookup.new
lookup.build_index("data/IP2LOCATION-LITE-DB3.zip")
puts "saving index..."
lookup.export_index
puts "done."
