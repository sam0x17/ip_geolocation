require "csv"
require "zip"
require "baked_file_system"
require "marshal"

module IPGeolocation
  class Storage
    extend BakedFileSystem
    bake_folder "../data"
  end

  class Lookup
    @mapping = Hash(Range(UInt32, UInt32), UInt64).new
    @keys = Array(Range(UInt32, UInt32)).new
    @locations = Hash(UInt64, Location).new

    delegate :size, to: @mapping

    def find(value : UInt32) : (Location | Nil)
      matched_range = @keys.bsearch do |range|
        value <= range.end
      end

      if matched_range && matched_range.includes?(value)
        digest = @mapping[matched_range]
        @locations[digest]
      end
    end

    def build_index(file_path)
      Zip::File.open(file_path.not_nil!) do |zip_file|
        zip_file.entries.first.open { |io| process_index_file(io) }
      end
    end

    def export_index(file_path = "./output.dat")
      bytes = {@mapping, @keys, @locations}.marshal_pack
      File.write(file_path, bytes)
    end

    private def process_index_file(io)
      chunk_size = 100000

      indexed_records_count = 0
      io.each_line.each_slice(chunk_size) do |slice|
        chunk = slice.join("\n")
        parsed_csv = CSV.parse(chunk)

        parsed_csv.each do |row|
          location = Location.new(row[2], row[3], row[4], row[5])
          digest = location.hash
          @locations[digest] = location
          @mapping[row[0].to_u32..row[1].to_u32] = digest
        end

        indexed_records_count += slice.size
      end
      @keys = @mapping.keys
    end
  end
end
