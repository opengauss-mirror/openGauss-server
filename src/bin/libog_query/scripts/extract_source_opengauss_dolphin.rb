# rubocop:disable all

# You need to call this with the PostgreSQL source directory as the first commandline agument, and the output dir as the second
# ruby scripts/extract_source_opengauss_dolphin.rb /opt/openGauss-server  /opt/binarylibs  /opt/openGauss-server/contrib/dolphin/libog_query/opengauss/source

require 'ffi/clang'
require 'json'
require 'fileutils'

module FFI::Clang::Lib
  enum :storage_class, [
    :invalid, 0,
    :none, 1,
    :extern, 2,
    :static, 3,
    :private_extern, 4,
    :opencl_workgroup_local, 5,
    :auto, 6,
    :register, 7,
  ]

  attach_function :get_storage_class, :clang_Cursor_getStorageClass, [FFI::Clang::Lib::CXCursor.by_value], :storage_class
end

module FFI::Clang
  class Cursor
    def storage_class
      Lib.get_storage_class(@cursor)
    end

    # Copy of clang::VarDecl::hasExternalStorage http://clang.llvm.org/doxygen/Decl_8h_source.html#l00982
    def has_external_storage
      storage_class == :extern || storage_class == :private_extern
    end
  end
end

class Runner
  attr_reader :unresolved
  attr_reader :code_for_resolve
  attr_reader :compatibility_mode

  def initialize
    @file_analysis = {}
    @global_method_to_base_filename = {}
    @file_to_method_and_pos = {}
    @external_variables = []
    @static_symbols_include = {}
    @resolved_static_by_base_filename = {}
    @resolved_global = []

    @symbols_to_output = {}
    @include_files_to_output = []
    @unresolved = []

    @blocklist = []
    @mock = {}

    @basepath = File.absolute_path(ARGV[0]) + '/'
    @thirdpartpath = File.absolute_path(ARGV[1]) + '/'
    @out_path = File.absolute_path(ARGV[2]) + '/'
    @compatibility_mode = ARGV[3]
  end

  def blocklist(symbol)
    @blocklist << symbol
  end

  def mock(symbol, code)
    @mock[symbol] = code
  end

  def generate_files(compatibility_mode, basepath)
    files = []
    common_files = [
      'src/gausskernel/process/threadpool/knl_session.cpp', # seLt_is_create_plsql_type etc
      'src/gausskernel/process/threadpool/knl_thread.cpp', # seLt_is_create_plsql_type etc
      'src/gausskernel/storage/ipc/shmem.cpp',
      'src/gausskernel/storage/access/hash/hash.cpp',
      'src/gausskernel/storage/access/hash/hashfunc.cpp',
      'src/common/backend/lib/dllist.cpp', # bit_in etc
      'src/common/backend/utils/mb/wchar.cpp',
      'src/common/backend/utils/mmgr/*.cpp',
      'src/common/backend/utils/hash/*.cpp',
      'src/common/backend/utils/error/elog.cpp',
      'src/common/backend/utils/error/assert.cpp',
      'src/common/backend/utils/init/globals.cpp',
      'src/common/backend/nodes/bitmapset.cpp',
      'src/common/backend/nodes/copyfuncs.cpp',
      'src/common/backend/nodes/equalfuncs.cpp',
      'src/common/backend/nodes/nodeFuncs.cpp',
      'src/common/backend/nodes/outfuncs.cpp',
      'src/common/backend/nodes/makefuncs.cpp',
      'src/common/backend/nodes/value.cpp',
      'src/common/backend/nodes/list.cpp',
      'src/common/backend/lib/stringinfo.cpp',
      'src/common/port/pgstrcasecmp.cpp',
      'src/common/port/qsort.cpp',
      'src/common/backend/utils/mb/encnames.cpp',
      'src/common/port/gs_strerror.cpp', # gs_strerror etc
      'src/common/port/strlcpy.cpp',
      'src/common/backend/catalog/namespace.cpp', # NameListToString etc
      'src/gausskernel/optimizer/commands/define.cpp'  # defWithOids etc
    ]

    if compatibility_mode == 'A'
      additional_files = [
        'src/common/backend/parser/*.cpp', # raw_parser
        'src/common/backend/utils/adt/varbit.cpp',
        'src/common/backend/utils/adt/arrayfuncs.cpp', # array_iterate etc
        'src/common/backend/utils/adt/ruleutils.cpp',
        'src/common/backend/utils/mb/mbutils.cpp',
        'src/common/backend/utils/adt/datum.cpp',
        'src/common/backend/utils/adt/name.cpp',
        'src/common/backend/utils/adt/varlena.cpp',
        'src/common/backend/utils/adt/numutils.cpp',
        'src/gausskernel/process/tcop/postgres.cpp', # checkCompArgs etc
        'src/common/backend/utils/mmgr/portalmem.cpp'
      ]
    elsif compatibility_mode == 'B'
      additional_files = [
        'contrib/dolphin/plugin_parser/*.cpp',
        'contrib/dolphin/plugin_utils/adt/varbit.cpp',
        'contrib/dolphin/plugin_utils/adt/arrayfuncs.cpp',
        'contrib/dolphin/plugin_utils/adt/ruleutils.cpp',
        'contrib/dolphin/plugin_utils/mb/mbutils.cpp',
        'contrib/dolphin/plugin_utils/adt/datum.cpp',
        'contrib/dolphin/plugin_utils/adt/name.cpp',
        'contrib/dolphin/plugin_utils/adt/varlena.cpp',
        'contrib/dolphin/plugin_utils/adt/numutils.cpp',
        'contrib/dolphin/plugin_postgres.cpp',
        'contrib/dolphin/plugin_utils.cpp'
      ]
    else
      raise "Invalid compatibility mode: #{compatibility_mode}"
    end
  
    additional_files.each do |file|
      files += Dir.glob(basepath + file)
    end

    common_files.each do |file|
      files += Dir.glob(basepath + file)
    end
  
    files
  end

  def run
    files = generate_files(@compatibility_mode, @basepath)
    total = files.size
    progress = 0
    files.each do |file|
      if files == [file]
        puts format('Analysing single file: %s', file)
        analysis = analyze_file(file)
        analysis_file = analysis.save
        puts format('Result: %s', analysis_file)
        exit 1
      end
      progress += 1
      puts format('progress file: %s', file)
      print "\r[#{'=' * (progress * 100 / total)} #{' ' * (100 - (progress * 100 / total))}] [#{progress}/#{total}]#{progress * 100 / total}%\n"


      analysis = FileAnalysis.restore(file, @basepath) || analyze_file(file)
      analysis.save

      @file_analysis[file] = analysis

      analysis.symbol_to_file.each do |symbol, _|
        next if analysis.static_symbols.include?(symbol)
        if @global_method_to_base_filename[symbol] && !['main', 'Pg_magic_func', 'pg_open_tzfile', '_PG_init'].include?(symbol) && !@global_method_to_base_filename[symbol].end_with?('cpp')
          puts format('Error processing %s, symbol %s already defined by %s', file, symbol, @global_method_to_base_filename[symbol])
        end
        @global_method_to_base_filename[symbol] = file
      end

      analysis.file_to_symbol_positions.each do |file, method_and_pos|
        @file_to_method_and_pos[file] = method_and_pos
      end

      analysis.external_variables.each do |symbol|
        @external_variables << symbol
      end
    end

    #puts @caller_to_static_callees['/Users/lfittl/Code/libpg_query/postgres/src/backend/regex/regc_locale.c']['cclass'].inspect

    puts "\nFinished parsing"
  end

  class FileAnalysis
    attr_accessor :references, :static_symbols, :symbol_to_file, :file_to_symbol_positions, :external_variables, :included_files

    def initialize(filename, basepath, references = {}, static_symbols = [],
      symbol_to_file = {}, file_to_symbol_positions = {}, external_variables = [],
      included_files = [])
      @filename = filename
      @basepath = basepath
      @references = references
      @static_symbols = static_symbols
      @symbol_to_file = symbol_to_file
      @file_to_symbol_positions = file_to_symbol_positions
      @external_variables = external_variables
      @included_files = included_files
    end

    def save
      json = JSON.pretty_generate({
        references: @references,
        static_symbols: @static_symbols,
        symbol_to_file: @symbol_to_file,
        file_to_symbol_positions: @file_to_symbol_positions,
        external_variables: @external_variables,
        included_files: @included_files,
      })

      file = self.class.analysis_filename(@filename, @basepath)
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, json)
      file
    end

    def self.restore(filename, basepath)
      json = File.read(analysis_filename(filename, basepath))
      hsh = JSON.parse(json)
      new(filename, basepath, hsh['references'], hsh['static_symbols'],
      hsh['symbol_to_file'], hsh['file_to_symbol_positions'], hsh['external_variables'],
      hsh['included_files'])
    rescue Errno::ENOENT
      nil
    end

    private

    def self.analysis_filename(filename, basepath)
      File.absolute_path('./opengauss/analysis') + '/' + filename.gsub(%r{^#{basepath}}, '').gsub(/.c$/, '.json')
    end
  end

  def analyze_file(file)
    index = FFI::Clang::Index.new(true, true)
    with_dolphin = file.start_with?(@basepath + 'contrib/dolphin/')
    flags = [
      '-I', @basepath + 'src/include',
      '-I', @thirdpartpath + 'buildtools/gcc10.3/gcc/lib/gcc/aarch64-unknown-linux-gnu/10.3.1/include/',
      '-I', @thirdpartpath + 'buildtools/gcc10.3/gcc/lib/gcc/aarch64-unknown-linux-gnu/10.3.1/include-fixed/',
      '-I', @thirdpartpath + 'kernel/dependency/libobs/comm/include',
      '-I', @thirdpartpath + 'kernel/dependency/cjson/comm/include',
      '-I', @basepath + 'contrib/dolphin/include',
      '-I', @basepath + 'contrib/dolphin/',
      '-DDLSUFFIX=".so"',
      '-g',
      '-DUSE_ASSERT_CHECKING',
      '-DMEMORY_CONTEXT_CHECKING',
      '-D_GNU_SOURCE',
      '-w',
      '-ferror-limit=0',
      '-DPGXC',
      '-DENABLE_MOT',
      '-w',
      '-DENABLE_HTAP'
    ]
    if with_dolphin
      flags += ['-DDOLPHIN']
    end
    translation_unit = index.parse_translation_unit(file, flags)

    cursor = translation_unit.cursor

    func_cursor = nil
    analysis = FileAnalysis.new(file, @basepath)

    included_files = []
    translation_unit.inclusions do |included_file, _inclusions|
      next if !included_file.start_with?(@basepath) || included_file == file
      included_files << included_file
    end
    analysis.included_files = included_files.uniq.sort
    cursor.visit_children do |cursor, parent|
      if cursor.location.file && (File.dirname(file) == File.dirname(cursor.location.file) || cursor.location.file.end_with?('_impl.h'))
        if parent.kind == :cursor_translation_unit
          if (cursor.kind == :cursor_function && cursor.definition?) || (cursor.kind == :cursor_variable && !cursor.has_external_storage) || (cursor.kind == :cursor_cxx_method && cursor.definition?) || (cursor.kind == :cursor_function_template && cursor.definition?) || (cursor.kind == :cursor_constructor && cursor.definition?)
            spelling = cursor.spelling

            if with_dolphin && @global_method_to_base_filename[spelling] && cursor.linkage == :external
              spelling = 'dolphin_' + cursor.spelling
            end

            if cursor.kind == :cursor_cxx_method || cursor.kind == :cursor_function_template || cursor.kind == :cursor_constructor
              if cursor.semantic_parent.kind == :cursor_class_decl
                spelling = cursor.semantic_parent.spelling + '::' + spelling
              end
            end
            analysis.symbol_to_file[spelling] = cursor.location.file

            if cursor.linkage == :external
              # Nothing special
            elsif cursor.linkage == :internal
              (analysis.static_symbols << spelling).uniq!
            else
              fail format('Unknown linkage: %s', cursor.linkage.inspect)
            end

            start_offset = cursor.extent.start.offset
            end_offset = cursor.extent.end.offset
            end_offset += 1 if cursor.kind == :cursor_variable # The ";" isn't counted correctly by clang

            if cursor.kind == :cursor_variable && (cursor.linkage == :external || cursor.linkage == :internal) &&
              !cursor.type.const_qualified? && !cursor.type.array_element_type.const_qualified? &&
              cursor.type.pointee.kind != :type_function_proto
              analysis.external_variables << spelling
            end
            analysis.file_to_symbol_positions[cursor.location.file] ||= {}
            # tempalate will be loop twice
            if  analysis.file_to_symbol_positions[cursor.location.file][spelling] == nil
              analysis.file_to_symbol_positions[cursor.location.file][spelling] = [start_offset, end_offset]

              last_cursor_kind = :cursor_unexposed_decl
              last_cursor_spelling = ''
              cursor.visit_children do |child_cursor, parent|
                # There seems to be a bug here on modern Clang versions where the
                # cursor kind gets modified once we call "child_cursor.definition"
                # - thus we make a copy ahead of calling that, for later use
                child_cursor_kind = child_cursor.kind

                # Ignore variable definitions from the local scope
                next :recurse if child_cursor.definition.semantic_parent == cursor
                # if spelling == 'GenericMemoryAllocator::AllocSetMethodDefinition' && child_cursor.spelling != ''
                #   print child_cursor.kind.to_s + ':' + child_cursor.spelling + "\n"
                # end
                if child_cursor_kind == :cursor_decl_ref_expr || child_cursor_kind == :cursor_call_expr || child_cursor_kind == :cursor_overloaded_decl_ref

                  cur_spelling = child_cursor.spelling
                  if child_cursor.semantic_parent.kind == :cursor_class_decl
                    cur_spelling = child_cursor.semantic_parent.spelling + '::' + cur_spelling
                  elsif last_cursor_kind && last_cursor_kind == :cursor_class_decl && child_cursor_kind == :cursor_overloaded_decl_ref
                    cur_spelling = last_cursor_spelling + '::' + cur_spelling
                  end
                  analysis.references[spelling] ||= []
                  (analysis.references[spelling] << cur_spelling).uniq!
                end
                last_cursor_kind = child_cursor.kind
                last_cursor_spelling = child_cursor.spelling
              :recurse
              end
            end
          end
        end
      end

      next :recurse
    end

    # cursor.visit_children is not always ordered. do it yourself: mbutils.cpp
    if analysis.file_to_symbol_positions[file]
      sort_result = analysis.file_to_symbol_positions[file].sort_by { |symbol_to_position | symbol_to_position[1] }
      tmp_result ||= {}
      sort_result.each do | elem |
        tmp_result[elem[0]] = [ elem[1][0], elem[1][1]]
      end
      analysis.file_to_symbol_positions[file] = tmp_result
    end
    analysis
  end

  RESOLVE_MAX_DEPTH = 100

  def add_static_symbols(file_name, symbols_name)
    base_filename = @basepath + file_name
    analysis = @file_analysis[base_filename]
    fail "could not find analysis data for #{base_filename}\##{symbols_name}" if analysis.nil?

    implementation_filename = analysis.symbol_to_file[symbols_name]
    if !implementation_filename
      (@unresolved << symbols_name).uniq!
      return
    end
    @symbols_to_output[implementation_filename] ||= []
    @symbols_to_output[implementation_filename] << symbols_name

    @static_symbols_include[implementation_filename] ||= symbols_name
  end

  def add_symbols(symbol_name)
    base_filename = @global_method_to_base_filename[symbol_name]
    if !base_filename
      (@unresolved << symbol_name).uniq!
      return
    end
    analysis = @file_analysis[base_filename]
    fail "could not find analysis data for #{base_filename}" if analysis.nil?


    # We need to determine if we can lookup the place where the method lives
    implementation_filename = analysis.symbol_to_file[symbol_name]
    if !implementation_filename
      (@unresolved << symbol_name).uniq!
      return
    end

    @symbols_to_output[implementation_filename] ||= []
    @symbols_to_output[implementation_filename] << symbol_name
  end

  def deep_resolve(method_name, depth: 0, trail: [], global_resolved_by_parent: [], static_resolved_by_parent: [], static_base_filename: nil)
    if @blocklist.include?(method_name)
      puts 'ERROR: Hit blocklist entry ' + method_name
      puts 'Trail: ' + trail.inspect
      exit 1
    end

    if depth > RESOLVE_MAX_DEPTH
      puts 'ERROR: Exceeded max depth'
      puts method_name.inspect
      puts trail.inspect
      exit 1
    end

    base_filename = static_base_filename || @global_method_to_base_filename[method_name]
    if !base_filename
      (@unresolved << method_name).uniq!
      return
    end

    analysis = @file_analysis[base_filename]
    fail "could not find analysis data for #{base_filename}" if analysis.nil?

    # We need to determine if we can lookup the place where the method lives
    implementation_filename = analysis.symbol_to_file[method_name]
    if !implementation_filename
      (@unresolved << method_name).uniq!
      return
    end

    @symbols_to_output[implementation_filename] ||= []
    @symbols_to_output[implementation_filename] << method_name

    (@include_files_to_output += analysis.included_files).uniq!

    if @mock.key?(method_name)
      # Code will be overwritten at output time, no need to investigate dependents
      return
    end

    # Now we need to resolve all symbols called by this one
    dependents = (analysis.references[method_name] || [])
    global_dependents = dependents.select { |c| !analysis.static_symbols.include?(c) } - global_resolved_by_parent
    static_dependents = dependents.select { |c| analysis.static_symbols.include?(c) } - static_resolved_by_parent

    # First, make sure we exclude all that have been visited before
    @resolved_static_by_base_filename[base_filename] ||= []
    global_dependents.delete_if { |s| @resolved_global.include?(s) }
    static_dependents.delete_if { |s| @resolved_static_by_base_filename[base_filename].include?(s) }

    # Second, make sure we never visit any of the dependents again
    global_dependents.each { |s| @resolved_global << s }
    static_dependents.each { |s| @resolved_static_by_base_filename[base_filename] << s }

    # Third, actually traverse into the remaining, non-visited, dependents
    global_dependents.each do |symbol|
      deep_resolve(
        symbol, depth: depth + 1, trail: trail + [method_name],
        global_resolved_by_parent: global_resolved_by_parent + global_dependents
      )
    end

    static_dependents.each do |symbol|
      deep_resolve(
        symbol, depth: depth + 1, trail: trail + [method_name],
        global_resolved_by_parent: global_resolved_by_parent + global_dependents,
        static_resolved_by_parent: static_resolved_by_parent + static_dependents,
        static_base_filename: base_filename
      )
    end
  end

  def special_include_file?(filename)
    filename[/\/(reg(c|e)_[\w_]+|guc-file|qsort_tuple|repl_scanner|levenshtein|bootscanner|like_match)\.c$/] || filename[/\/[\w_]+\.funcs.c$/] || filename[/\/[\w_]+_impl.h$/]
  end

  def write_out
    all_thread_local_variables = []
    count = 1
    @symbols_to_output.each do |filename, symbols|
      # next if filename.end_with?('portalmem.cpp') == false
      next if filename.end_with?('rackset.cpp')
      file_thread_local_variables = []
      dead_positions = (@file_to_method_and_pos[filename] || {}).dup

      symbols.each do |symbol|
        next if @mock.key?(symbol)
        next if @external_variables.include?(symbol)
        next if @static_symbols_include[filename] && @static_symbols_include[filename].include?(symbol)

        alive_pos = dead_positions[symbol]

        # In some cases there are other symbols at the same location (macros), so delete by position instead of name
        dead_positions.delete_if { |_,pos| pos == alive_pos }
      end

      full_code = File.read(filename)
      printf  count.to_s + filename + "\n"
      count += 1
      str = "/*--------------------------------------------------------------------\n"
      str += " * Symbols referenced in this file:\n"
      symbols.each do |symbol|
        str += format(" * - %s\n", symbol)
      end
      str += " *--------------------------------------------------------------------\n"
      str += " */\n\n"

      next_start_pos = 0
      dead_positions.each do |symbol, pos|
        fail format("Position overrun for %s in %s, next_start_pos (%d) > file length (%d)", symbol, filename, next_start_pos, full_code.size) if next_start_pos > full_code.size
        fail format("Position overrun for %s in %s, dead position pos[0]-1 (%d) > file length (%d)", symbol, filename, pos[0]-1, full_code.size) if pos[0]-1 > full_code.size

        if symbol.end_with?('ResetPortalCursor') || symbol.end_with?('HoldPinnedPortals') ||
           symbol.end_with?('CursorRecordTypeUnbind') || symbol.end_with?('InitDolphinProcBodyInfo') ||
           symbol.end_with?('DolphinDealProcBodyStr') || symbol.end_with?('ParseFloatByExtentedPrecision') ||
           symbol.end_with?('CheckTypmodScale') || symbol.end_with?('make_AStar_subquery') ||
           symbol.end_with?('get_arg_mode_by_name') || symbol.end_with?('get_arg_mode_by_pos') ||
           symbol.end_with?('CheckTypmodScale') || symbol.end_with?('make_AStar_subquery') ||
           symbol.end_with?('get_table_modes') || symbol.end_with?('append_inarg_list') ||
           symbol.end_with?('check_outarg_info') || symbol.end_with?('HasVariadic') ||
           symbol.end_with?('makeCallFuncStmt') || symbol.end_with?('IsValidGroupname')
          str += full_code[next_start_pos...(pos[0]-2)]
          skipped_code = full_code[(pos[0]-2)...pos[1]]      
        else
          str += full_code[next_start_pos...(pos[0]-1)]
          skipped_code = full_code[(pos[0]-1)...pos[1]]
        end

        if @mock.key?(symbol)
          str += "\n" + @mock[symbol] + "\n"
        elsif @external_variables.include?(symbol) && symbols.include?(symbol)
          file_thread_local_variables << symbol
          # openGauss is thread model process. Th variables are always thread-safe.
          str += "\n " + skipped_code.strip + "\n"
        elsif @static_symbols_include[filename] && @static_symbols_include[filename].include?(symbol)
           str += "\n " + skipped_code.strip + "\n"
       else
          # In the off chance that part of a macro is before a symbol (e.g. ifdef),
          # but the closing part is inside (e.g. endif) we need to output all macros inside skipped parts
          str += "\n" + skipped_code.scan(/^(#\s*(?:define|undef|if|ifdef|ifndef|else|endif))((?:[^\n]*\\\s*\n)*)([^\n]*)$/m).map { |m| m.compact.join }.join("\n")
        end

        next_start_pos = pos[1]
      end
       if full_code[next_start_pos..-1]
        str += full_code[next_start_pos..-1]
      end
      # In some cases we also need to take care of definitions in the same file
      file_thread_local_variables.each do |variable|
        str.gsub!(/(PGDLLIMPORT|extern)\s+(const|volatile)?\s*(\w+)\s+(\*{0,2})#{variable}(\[\])?;/, "\\1 \\2 \\3 \\4#{variable}\\5;")
      end
      all_thread_local_variables += file_thread_local_variables
      if filename.end_with?('postgres.cpp') || filename.end_with?('plugin_postgres.cpp') || filename.end_with?('plugin_utility.cpp')
        # remove boost dependency
        str = str.gsub(%r{^#include.*gs_policy/(gs_policy_(masking|audit)|policy_common)\.h.*}, '')
      elsif filename.end_with?('threadpool_controler.cpp')
        str = str.gsub(%r{^#include.*communication/commproxy_interface.h".*}, '')
      end

      unless special_include_file?(filename)
        out_name = filename.gsub(%r{^#{@basepath}}, '').gsub('/', '_')
        File.write(@out_path + out_name, str)
      end
    end

    #return
    additional_includes = Dir.glob(@thirdpartpath + 'kernel/dependency/libobs/comm/include/eSDKOBS.h') +
      Dir.glob(@thirdpartpath + 'kernel/dependency/cjson/comm/include/cjson/cJSON.h') +
      Dir.glob(@basepath + 'src/include/utils/aset.h')

    (@include_files_to_output + additional_includes).each do |include_file|
        # printf include_file + "\n"
      # next if include_file.end_with?('gram.hpp') == false
      if include_file.start_with?(@basepath + 'src/include') #|| include_file.start_with?(@basepath + 'contrib/dolphin/')
        out_file = @out_path + include_file.gsub(%r{^#{@basepath}src/}, '')
      elsif include_file.start_with?(@thirdpartpath + 'kernel/dependency')
        out_file = @out_path + 'include/' + include_file.gsub(%r{^#{@thirdpartpath}.*?include/}, '')
      elsif include_file.start_with?(@basepath + 'contrib/dolphin/include')
        out_file = @out_path + 'include/' + include_file.gsub(%r{^#{@basepath}.*?include/}, '')
      elsif include_file.start_with?(@basepath + 'contrib/dolphin/') && !include_file.end_with?('scan.inc') && !include_file.end_with?('hint_scan.inc')
        out_file = @out_path + 'include/' + include_file.gsub(%r{^#{@basepath}.*?dolphin/}, '')
      else
        out_file = @out_path + 'include/' + File.basename(include_file)
      end

      code = File.read(include_file)
      all_thread_local_variables.each do |variable|
        code.gsub!(/(extern\s+)(PGDLLIMPORT\s+)?(const\s+)?(volatile\s+)?(\w+)\s+(\*{0,2})#{variable}(\[\])?;/, "\\1\\2 \\3\\4\\5 \\6#{variable}\\7;")
      end

      FileUtils.mkdir_p File.dirname(out_file)
      File.write(out_file, code)
    end
  end

  def compress
    folder_to_compress = @out_path
    output_file = 'source_dolphin.tar.gz'
    if @compatibility_mode == 'A'
      output_file = 'source_server.tar.gz'
    end
    destination_path = @basepath + 'src/bin/libog_query/'
    tar_command = "tar -zcf #{output_file} -C #{File.dirname(folder_to_compress)} #{File.basename(folder_to_compress)}"
    system(tar_command)
    if $?.success?
      puts "compress success: #{destination_path}#{output_file}"
    else
      puts "compress error!"
    end
  end
end

runner = Runner.new
runner.run

runner.blocklist('heap_open')
runner.blocklist('relation_open')
runner.blocklist('RelnameGetRelid')
runner.blocklist('ProcessClientWriteInterrupt')
runner.blocklist('typeStringToTypeName')
runner.blocklist('LWLockAcquire')
runner.blocklist('SPI_freeplan')
runner.blocklist('get_ps_display')
runner.blocklist('pq_beginmessage')
runner.blocklist('GlobalSysDBCache')

# We have to mock this as it calls `hash_search`, which eventually makes
# calls down to `pgstat_report_wait_start` and `pgstat_report_wait_end`.
# These functions depend on the existence of a global variable
# `my_wait_event_info`.
#
# The problem here is we can't reasonably compile the source file
# `backend/utils/activity/wait_event.c` which provides this symbol, as it
# initializes the value by-default to a reference to another global.
# Normally this is fine, but we transform both of these globals so that they
# are thread local via `THR_LOCAL`, and it is not valid to initialize one thread
# local with the address of another.
#
# Instead of tackling this directly, we just return `NULL` in the mock below,
# observing that we do not need to support the registration of custom nodes.
runner.mock('GetExtensibleNodeMethods', %(
const ExtensibleNodeMethods *
GetExtensibleNodeMethods(const char *extnodename, bool missing_ok)
{
	return NULL;
}
))

# Mocks REQUIRED for sysCache
runner.mock('SearchSysCache', 'HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4, int level) {return NULL;}')
runner.mock('ReleaseSysCache', 'void ReleaseSysCache(HeapTuple tuple) {return NULL;}')
runner.mock('DispatchSession', 'int ThreadPoolControler::DispatchSession(Port* port) {return 0;}')


runner.mock('LookupTypeName', 'Type LookupTypeName(ParseState *pstate, const TypeName *typeName, int32 *typmod_p, bool print_notice, TypeDependExtend* dependExtend){return NULL;}')
runner.mock('HeapOpenrvExtended', 'Relation HeapOpenrvExtended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok, bool isSupportSynonym, StringInfo detailInfo){return NULL;}')
runner.mock('AdjustThreadAffinity', 'void AdjustThreadAffinity(void) {}')

# todo
runner.mock('makeCallFuncStmt', 'static Node *makeCallFuncStmt(List* funcname,List* parameters, bool is_call){return NULL;}')
runner.mock('GetUserId', 'Oid GetUserId(void){return 0;}')
runner.mock('pg_server_to_client', 'char* pg_server_to_client(const char* s, int len){return (char*)s;}')
# runner.mock('recomputeNamespacePath', 'Oid GetUserId(void){return 0;}')

# executor.h#AutoMutexLock#unLock
runner.mock('ResourceOwnerForgetIfExistPthreadMutex', 'int ResourceOwnerForgetIfExistPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex, bool trace){return 0;}')
runner.mock('InitSpiPrinttupDR', 'void InitSpiPrinttupDR(DestReceiver* dr) {return;}')
# exclude OgRecordStat
runner.mock('knl_u_stat_init', 'static void knl_u_stat_init(knl_u_stat_context* stat_cxt) {return;}')

# Mocks REQUIRED for basic operations (error handling, memory management)
runner.mock('ProcessInterrupts', 'void ProcessInterrupts(void) {}') # Required by errfinish
runner.mock('PqCommMethods', 'const PQcommMethods *PqCommMethods = NULL;') # Required by errfinish
runner.mock('send_message_to_server_log', 'static void send_message_to_server_log(ErrorData *edata) {}')
runner.mock('send_message_to_frontend', 'void send_message_to_frontend(ErrorData *edata) {}')
runner.mock('knl_t_libpq_init', 'static void knl_t_libpq_init(knl_t_libpq_context* libpq_cxt) {}')

# Mocks REQUIRED for PL/pgSQL parsing
runner.mock('format_type_be', 'char * format_type_be(Oid type_oid) { return pstrdup("-"); }')
runner.mock('build_row_from_class', 'static PLpgSQL_row *build_row_from_class(Oid classOid) { return NULL; }')
runner.mock('plpgsql_build_datatype', 'PLpgSQL_type * plpgsql_build_datatype(Oid typeOid, int32 typmod, Oid collation, TypeName *origtypname) { PLpgSQL_type *typ; typ = (PLpgSQL_type *) palloc0(sizeof(PLpgSQL_type)); typ->typname = pstrdup("UNKNOWN"); typ->ttype = PLPGSQL_TTYPE_SCALAR; return typ; }')
runner.mock('parse_datatype', 'static PLpgSQL_type * parse_datatype(const char *string, int location) { PLpgSQL_type *typ; typ = (PLpgSQL_type *) palloc0(sizeof(PLpgSQL_type)); typ->typname = pstrdup(string); typ->ttype = strcmp(string, "RECORD") == 0 ? PLPGSQL_TTYPE_REC : PLPGSQL_TTYPE_SCALAR; return typ; }')
runner.mock('get_collation_oid', 'Oid get_collation_oid(List *name, bool missing_ok) { return -1; }')
runner.mock('plpgsql_parse_wordtype', 'PLpgSQL_type * plpgsql_parse_wordtype(char *ident) { return NULL; }')
runner.mock('plpgsql_parse_wordrowtype', 'PLpgSQL_type * plpgsql_parse_wordrowtype(char *ident) { return NULL; }')
runner.mock('plpgsql_parse_cwordtype', 'PLpgSQL_type * plpgsql_parse_cwordtype(List *idents) { return NULL; }')
runner.mock('plpgsql_parse_cwordrowtype', 'PLpgSQL_type * plpgsql_parse_cwordrowtype(List *idents) { return NULL; }')
runner.mock('function_parse_error_transpose', 'bool function_parse_error_transpose(const char *prosrc) { return false; }')
runner.mock('free_expr', "static void free_expr(PLpgSQL_expr *expr) {}") # This would free a cached plan, which does not apply to us
runner.mock('make_return_stmt', %(
static PLpgSQL_stmt *
make_return_stmt(int location)
{
	PLpgSQL_stmt_return *new;

  Assert(plpgsql_curr_compile->fn_rettype == VOIDOID);

	new = palloc0(sizeof(PLpgSQL_stmt_return));
	new->cmd_type = PLPGSQL_STMT_RETURN;
	new->lineno   = plpgsql_location_to_lineno(location);
	new->expr	  = NULL;
	new->retvarno = -1;

  int tok = yylex();

  if (tok != ';')
	{
		plpgsql_push_back_token(tok);
		new->expr = read_sql_expression(';', ";");
	}

	return (PLpgSQL_stmt *) new;
}
)) # We're always working with fn_rettype = VOIDOID, due to our use of plpgsql_compile_inline

# Mocks REQUIRED for Windows support
runner.mock('write_stderr', %(
void
write_stderr(const char *fmt,...)
{
	va_list	ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	fflush(stderr);
	va_end(ap);
}
)) # Avoid pulling in write_console/write_eventlog, and instead always output to stderr (like on POSIX)
runner.mock('should_output_to_client', 'static inline bool should_output_to_client(int elevel) { return false; }') # Avoid pulling in postmaster.c, which has a bunch of Windows-specific code hidden behind a define
runner.mock('pg_lltoa_n', 'static inline int pg_lltoa_n(int64 value, char* a) { return 0; }')

runner.mock('setTargetTable', 'int setTargetTable(ParseState* pstate, RangeVar* relRv, bool inh, bool alsoSource, AclMode requiredPerms, bool multiModify) {return 0;}')
runner.mock('coerce_to_boolean', 'Node* coerce_to_boolean(ParseState* pstate, Node* node, const char* constructName) {return nullptr;}')
runner.mock('ExpandColumnRefStar', 'static List* ExpandColumnRefStar(ParseState* pstate, ColumnRef* cref, bool targetlist) {return nullptr;}')
runner.mock('ExpandIndirectionStar', 'List* ExpandIndirectionStar(ParseState* pstate, A_Indirection* ind, bool targetlist, ParseExprKind exprKind) {return nullptr;}')
runner.mock('coerce_to_target_type', 'Node* coerce_to_target_type(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod, CoercionContext ccontext, CoercionForm cformat, char* fmtstr, char* nlsfmtstr, int location) {return nullptr;}')
runner.mock('expandRelation', 'static void expandRelation(Oid relid, Alias* eref, int rtindex, int sublevels_up, int location, bool include_dropped, List** colnames, List** colvars){return;}')
runner.mock('parserOpenTable', 'Relation parserOpenTable(ParseState *pstate, const RangeVar *relation, int lockmode, bool isFirstNode, bool isCreateView, bool isSupportSynonym) {return nullptr;}')
runner.mock('errorMissingRTE', 'void errorMissingRTE(ParseState* pstate, RangeVar* relation, bool hasplus) {return;}')
runner.mock('IsSequenceFuncCall', 'static bool IsSequenceFuncCall(Node* filed1, Node* filed2, Node* filed3) {return false;}')

runner.mock('typenameTypeIdAndMod', %(
void typenameTypeIdAndMod(ParseState* pstate, const TypeName* typname, Oid* typeid_p, int32* typmod_p, TypeDependExtend* dependExtend) 
{ 
 return;
}
))

runner.mock('FuncNameAsType', 'static Oid FuncNameAsType(List* funcname) {return InvalidOid;}')
runner.mock('AddDefaultExprNode', 'static void AddDefaultExprNode(ParseState* pstate) {return;}')
runner.mock('is_relation_child', 'static bool is_relation_child(RangeTblEntry* child_rte, List* rtable) {return false;}')
runner.mock('checkDeleteStmtForPlanTable', 'static bool checkDeleteStmtForPlanTable(List* relations) {return false;}')
runner.mock('transformLockingClause', 'static void transformLockingClause(ParseState* pstate, Query* qry, LockingClause* lc, bool pushedDown) {return;}')
runner.mock('get_column_def_collation_b_format', 'Oid get_column_def_collation_b_format(ColumnDef* coldef, Oid typeOid, Oid typcollation, bool is_bin_type, Oid rel_coll_oid) {return 0;}')
runner.mock('check_collation_by_charset', 'Oid check_collation_by_charset(const char* collate, int charset) {return 0;}')
runner.mock('parserOpenTable', 'Relation parserOpenTable(ParseState *pstate, const RangeVar *relation, int lockmode, bool isFirstNode, bool isCreateView, bool isSupportSynonym) {return nullptr;}')


runner.mock('make_const', 'Const* make_const(ParseState* pstate, Value* value, int location) {return nullptr;}')
runner.mock('ValidateDependView', 'static ValidateDependResult ValidateDependView(Oid view_oid, char objType, List** list, bool force){return ValidateDependInvalid;}')
runner.mock('CheckUnsupportInsertSelectClause', 'static void CheckUnsupportInsertSelectClause(Query* query) {return;}')

runner.mock('pg_analyze_and_rewrite', 'List* pg_analyze_and_rewrite(Node* parsetree, const char* query_string, Oid* paramTypes, int numParams, ParseState* parent_pstate) {return nullptr;}')
runner.mock('transformDeleteStmt', 'static Query* transformDeleteStmt(ParseState* pstate, DeleteStmt* stmt) {return nullptr;}')
runner.mock('transformInsertStmt', 'static Query* transformInsertStmt(ParseState* pstate, InsertStmt* stmt) {return nullptr;}')
runner.mock('transformUpdateStmt', 'static Query* transformUpdateStmt(ParseState* pstate, UpdateStmt* stmt) {return nullptr;}')
runner.mock('transformMergeStmt', 'Query* transformMergeStmt(ParseState* pstate, MergeStmt* stmt){return nullptr;}')
runner.mock('transformDeclareCursorStmt', 'static Query* transformDeclareCursorStmt(ParseState* pstate, DeclareCursorStmt* stmt){return nullptr;}')
runner.mock('transformExplainStmt', 'static Query* transformExplainStmt(ParseState* pstate, ExplainStmt* stmt){return nullptr;}')
runner.mock('transformExecDirectStmt', 'static Query* transformExecDirectStmt(ParseState* pstate, ExecDirectStmt* stmt){return nullptr;}')
runner.mock('transformCreateTableAsStmt', 'static Query* transformCreateTableAsStmt(ParseState* pstate, CreateTableAsStmt* stmt){return nullptr;}')
runner.mock('transformCreateModelStmt', 'Query* transformCreateModelStmt(ParseState* pstate, CreateModelStmt* stmt){return nullptr;}')
runner.mock('transformVariableSetValueStmt', 'static void transformVariableSetValueStmt(ParseState* pstate, VariableSetStmt* stmt){return;}')
runner.mock('transformVariableMutiSetStmt', 'static Query* transformVariableMutiSetStmt(ParseState* pstate, VariableMultiSetStmt* muti_stmt){return nullptr;}')
runner.mock('transformVariableCreateEventStmt', 'Query* transformVariableCreateEventStmt(ParseState* pstate, CreateEventStmt* stmt){return nullptr;}')
runner.mock('transformVariableAlterEventStmt', 'Query* transformVariableAlterEventStmt(ParseState* pstate, AlterEventStmt* stmt){return nullptr;}')
runner.mock('TransformCompositeTypeStmt', 'static Query* TransformCompositeTypeStmt(ParseState* pstate, CompositeTypeStmt* stmt){return nullptr;}')

## dolphin
runner.mock('findCreateTrigger', 'SelectStmt *findCreateTrigger(RangeVar *trel){return nullptr;}')
runner.mock('current_schema', 'Datum current_schema(FunctionCallInfo fcinfo) {return 0;}')
runner.mock('get_rel_relkind', 'char get_rel_relkind(Oid relid) { return '';}')

runner.mock('init_session_vars', %(
void init_session_vars(void)
{
    int dolphin_index = 0;
    RepallocSessionVarsArrayIfNecessary();

    BSqlPluginContext *cxt = (BSqlPluginContext *) MemoryContextAlloc(u_sess->self_mem_cxt, sizeof(bSqlPluginContext));
    u_sess->attr.attr_common.extension_session_vars_array[dolphin_index] = cxt;
    cxt->enableBCmptMode = false;
    cxt->lockNameList = NIL;
    cxt->scan_from_pl = false;
    cxt->is_b_declare = false;
    cxt->default_database_name = NULL;
    cxt->mysql_ca = NULL;
    cxt->mysql_server_cert = NULL;
    cxt->mysql_server_key = NULL;
    cxt->paramIdx = 0;
    cxt->isUpsert = false;
    cxt->single_line_trigger_begin = 0;
    cxt->do_sconst = NULL;
    cxt->single_line_proc_begin = 0;
    cxt->is_schema_name = false;
    cxt->b_stmtInputTypeHash = NULL;
    cxt->b_sendBlobHash = NULL;
    cxt->is_dolphin_call_stmt = false;
    cxt->is_binary_proto = false;
    cxt->is_ast_stmt = false;
    cxt->group_by_error = false;
    cxt->is_create_alter_stmt = false;
    cxt->isDoCopy = false;
    cxt->isInTransformSet = false;
    cxt->is_set_stmt = false;
    cxt->Conn_Mysql_Info = NULL;

    if (temp_Conn_Mysql_Info) {
        cxt->Conn_Mysql_Info = (conn_mysql_infoP_t)MemoryContextAllocZero(u_sess->self_mem_cxt,
                                                                          (Size)(sizeof(conn_mysql_info_t)));
        errno_t rc = memcpy_s(cxt->Conn_Mysql_Info, sizeof(conn_mysql_info_t),
                              temp_Conn_Mysql_Info, sizeof(conn_mysql_info_t));
        securec_check(rc, "\0", "\0");
        pfree(temp_Conn_Mysql_Info);
        temp_Conn_Mysql_Info = NULL;
    }
    u_sess->attr.attr_sql.sql_compatibility = B_FORMAT;
    u_sess->attr.attr_common.delimiter_name = ";";
    cxt->sqlModeFlags = cxt->sqlModeFlags | OPT_SQL_MODE_ANSI_QUOTES | OPT_SQL_MODE_STRICT |
    					OPT_SQL_MODE_PIPES_AS_CONCAT | OPT_SQL_MODE_NO_ZERO_DATE |
    					OPT_SQL_MODE_PAD_CHAR_TO_FULL_LENGTH | OPT_SQL_MODE_AUTO_RECOMPILE_FUNCTION |
    					OPT_SQL_MODE_ERROR_FOR_DIVISION_BY_ZERO;
}
))

runner.mock('GetSessionContext', %(
BSqlPluginContext* GetSessionContext()
{
    int dolphin_index = 0;
    if (!u_sess->attr.attr_common.extension_session_vars_array) {
        int initExtArraySize = 10;
        u_sess->attr.attr_common.extension_session_vars_array =
        (void**)MemoryContextAllocZero(u_sess->self_mem_cxt, (Size)(initExtArraySize * sizeof(void*)));
    }
        
    if (u_sess->attr.attr_common.extension_session_vars_array[dolphin_index] == NULL) {
        init_session_vars();
    }
    return (BSqlPluginContext *) u_sess->attr.attr_common.extension_session_vars_array[dolphin_index];
}
))

runner.mock('raw_expression_tree_walker', %(
bool raw_expression_tree_walker(Node* node, bool (*walker)(), void* context)
{
    ListCell* temp = NULL;
    bool (*p2walker)(void*, void*) = (bool (*)(void*, void*))walker;

    typedef struct json_walker_context {
      cJSON* root_obj;
      cJSON* cur_obj;
      cJSON* pre_obj;
      Node* parent_node;
    } json_walker_context;
    extern bool create_json_walker(Node* node, void* walker_context);

    /*
     * The walker has already visited the current node, and so we need only
     * recurse into any sub-nodes it has.
     */
    if (node == NULL) {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(node)) {
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
        case T_ParamRef:
        case T_A_Const:
        case T_A_Star:
        case T_Rownum:
            /* primitive node types with no subnodes */
            break;
        case T_Alias:
            /* we assume the colnames list isn't interesting */
            break;
        case T_RangeVar:
            return p2walker(((RangeVar*)node)->alias, context);
        case T_GroupingFunc:
            return p2walker(((GroupingFunc*)node)->args, context);
        case T_GroupingSet:
            return p2walker(((GroupingSet*)node)->content, context);
        case T_SubLink: {
            SubLink* sublink = (SubLink*)node;

            if (p2walker(sublink->testexpr, context)) {
                return true;
            }
            /* we assume the operName is not interesting */
            if (p2walker(sublink->subselect, context)) {
                return true;
            }
        } break;
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;

            if (p2walker(caseexpr->arg, context))
                return true;
            /* we assume walker doesn't care about CaseWhens, either */
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);

                Assert(IsA(when, CaseWhen));
                if (p2walker(when->expr, context)) {
                    return true;
                }
                if (p2walker(when->result, context)) {
                    return true;
                }
            }
            if (p2walker(caseexpr->defresult, context)){
                return true;
            }
        } break;
        case T_RowExpr:
            /* Assume colnames isn't interesting */
            return p2walker(((RowExpr*)node)->args, context);
        case T_CoalesceExpr:
            return p2walker(((CoalesceExpr*)node)->args, context);
        case T_MinMaxExpr:
            return p2walker(((MinMaxExpr*)node)->args, context);
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;

            if (p2walker(xexpr->named_args, context)) {
                return true;
            }
            /* we assume walker doesn't care about arg_names */
            if (p2walker(xexpr->args, context)) {
                return true;
            }
        } break;
        case T_NullTest:
            return p2walker(((NullTest*)node)->arg, context);
        case T_NanTest:
            return p2walker(((NanTest*)node)->arg, context);
        case T_InfiniteTest:
            return p2walker(((InfiniteTest*)node)->arg, context);
        case T_BooleanTest:
            return p2walker(((BooleanTest*)node)->arg, context);
        case T_HashFilter:
            return p2walker(((HashFilter*)node)->arg, context);
        case T_JoinExpr: {
            JoinExpr* join = (JoinExpr*)node;

            if (p2walker(join->larg, context)) {
                return true;
            }
            if (p2walker(join->rarg, context)) {
                return true;
            }
            if (p2walker(join->quals, context)) {
                return true;
            }
            if (p2walker(join->alias, context)) {
                return true;
            }
            /* using list is deemed uninteresting */
        } break;
        case T_IntoClause: {
            IntoClause* into = (IntoClause*)node;

            if (p2walker(into->rel, context)) {
                return true;
            }
            /* colNames, options are deemed uninteresting */
        } break;
        case T_List:
            foreach (temp, (List*)node) {
                if (p2walker((Node*)lfirst(temp), context)) {
                    return true;
                }
            }
            break;
        case T_InsertStmt: {
            InsertStmt* stmt = (InsertStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->cols, context)) {
                return true;
            }
            if (p2walker(stmt->selectStmt, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_DeleteStmt: {
            DeleteStmt* stmt = (DeleteStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->usingClause, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->limitClause, context)) {
                return true;
            }
            if (p2walker(stmt->relations, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_UpdateStmt: {
            UpdateStmt* stmt = (UpdateStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->targetList, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->fromClause, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->relationClause, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_MergeStmt: {
            MergeStmt* stmt = (MergeStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->source_relation, context)) {
                return true;
            }
            if (p2walker(stmt->join_condition, context)) {
                return true;
            }
            if (p2walker(stmt->mergeWhenClauses, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_MergeWhenClause: {
            MergeWhenClause* mergeWhenClause = (MergeWhenClause*)node;

            if (p2walker(mergeWhenClause->condition, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->targetList, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->cols, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->values, context)) {
                return true;
            }
        } break;
        case T_SelectStmt: {
            SelectStmt* stmt = (SelectStmt*)node;

            if (p2walker(stmt->distinctClause, context)) {
                return true;
            }
            if (p2walker(stmt->intoClause, context)) {
                return true;
            }
            if (p2walker(stmt->targetList, context)) {
                return true;
            }
            if (p2walker(stmt->fromClause, context)) {
                return true;
            }
            if (p2walker(stmt->unrotateInfo, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->groupClause, context)) {
                return true;
            }
            if (p2walker(stmt->havingClause, context)) {
                return true;
            }
            if (p2walker(stmt->windowClause, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->valuesLists, context)) {
                return true;
            }
            if (p2walker(stmt->sortClause, context)) {
                return true;
            }
            if (p2walker(stmt->limitOffset, context)) {
                return true;
            }
            if (p2walker(stmt->limitCount, context)) {
                return true;
            }
            if (p2walker(stmt->lockingClause, context)) {
                return true;
            }
            if (p2walker(stmt->larg, context)) {
                return true;
            }
            if (p2walker(stmt->rarg, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_A_Expr: {
            A_Expr* expr = (A_Expr*)node;

            if (p2walker(expr->lexpr, context)) {
                return true;
            }
            if (p2walker(expr->rexpr, context)) {
                return true;
            }
            /* operator name is deemed uninteresting */
        } break;
        case T_ColumnRef:
            /* we assume the fields contain nothing interesting */
            break;
        case T_FuncCall: {
            FuncCall* fcall = (FuncCall*)node;

            if (p2walker(fcall->args, context)) {
                return true;
            }
            if (p2walker(fcall->agg_order, context)) {
                return true;
            }
            if (p2walker(fcall->agg_filter, context)) {
                return true;
            }
            if (p2walker(fcall->aggKeep, context)) {
                return true;
            }
            if (p2walker(fcall->over, context)) {
                return true;
            }
            /* function name is deemed uninteresting */
        } break;
        case T_NamedArgExpr:
            return p2walker(((NamedArgExpr*)node)->arg, context);
        case T_A_Indices: {
            A_Indices* indices = (A_Indices*)node;

            if (p2walker(indices->lidx, context)) {
                return true;
            }
            if (p2walker(indices->uidx, context)) {
                return true;
            }
        } break;
        case T_A_Indirection: {
            A_Indirection* indir = (A_Indirection*)node;

            if (p2walker(indir->arg, context)) {
                return true;
            }
            if (p2walker(indir->indirection, context)) {
                return true;
            }
        } break;
        case T_A_ArrayExpr:
            return p2walker(((A_ArrayExpr*)node)->elements, context);
        case T_ResTarget: {
            ResTarget* rt = (ResTarget*)node;

            if (p2walker(rt->indirection, context)) {
                return true;
            }
            if (p2walker(rt->val, context)) {
                return true;
            }
        } break;
        case T_TypeCast: {
            TypeCast* tc = (TypeCast*)node;

            if (p2walker(tc->arg, context)) {
                return true;
            }
            if (p2walker(tc->typname, context)) {
                return true;
            }
            if (p2walker(tc->fmt_str, context)) {
                return true;
            }
            if (p2walker(tc->nls_fmt_str, context)) {
                return true;
            }
            if (p2walker(tc->default_expr, context)) {
                return true;
            }
        } break;
        case T_CollateClause:
            return p2walker(((CollateClause*)node)->arg, context);
        case T_SortBy:
            return p2walker(((SortBy*)node)->node, context);
        case T_WindowDef: {
            WindowDef* wd = (WindowDef*)node;

            if (p2walker(wd->partitionClause, context)) {
                return true;
            }
            if (p2walker(wd->orderClause, context)) {
                return true;
            }
            if (p2walker(wd->startOffset, context)) {
                return true;
            }
            if (p2walker(wd->endOffset, context)) {
                return true;
            }
        } break;
        case T_RangeSubselect: {
            RangeSubselect* rs = (RangeSubselect*)node;

            if (p2walker(rs->subquery, context)) {
                return true;
            }
            if (p2walker(rs->alias, context)) {
                return true;
            }
            if (p2walker(rs->rotate, context)) {
                return true;
            }
        } break;
        case T_RangeFunction: {
            RangeFunction* rf = (RangeFunction*)node;

            if (p2walker(rf->funccallnode, context)) {
                return true;
            }
            if (p2walker(rf->alias, context)) {
                return true;
            }
        } break;
        case T_RangeTableSample: {
            RangeTableSample* rts = (RangeTableSample*)node;

            if (p2walker(rts->relation, context)) {
                return true;
            }
            /* method name is deemed uninteresting */
            if (p2walker(rts->args, context)) {
                return true;
            }
            if (p2walker(rts->repeatable, context)) {
                return true;
            }
        } break;
        case T_RangeTimeCapsule: {
            RangeTimeCapsule* rtc = (RangeTimeCapsule*)node;

            if (p2walker(rtc->relation, context)) {
                return true;
            }
            /* method name is deemed uninteresting */
            if (p2walker(rtc->tvver, context)) {
                return true;
            }
        } break;
        case T_TypeName: {
            TypeName* tn = (TypeName*)node;

            if (p2walker(tn->typmods, context)) {
                return true;
            }
            if (p2walker(tn->arrayBounds, context)) {
                return true;
            }
            /* type name itself is deemed uninteresting */
        } break;
        case T_ColumnDef: {
            ColumnDef* coldef = (ColumnDef*)node;

            if (p2walker(coldef->typname, context)) {
                return true;
            }
            if (p2walker(coldef->raw_default, context)) {
                return true;
            }
            if (p2walker(coldef->collClause, context)) {
                return true;
            }
            /* for now, constraints are ignored */
        } break;
        case T_LockingClause:
            return p2walker(((LockingClause*)node)->lockedRels, context);
        case T_XmlSerialize: {
            XmlSerialize* xs = (XmlSerialize*)node;

            if (p2walker(xs->expr, context)) {
                return true;
            }
            if (p2walker(xs->typname, context)) {
                return true;
            }
        } break;
        case T_WithClause:
            return p2walker(((WithClause*)node)->ctes, context);
        case T_RotateClause:  {
            RotateClause *stmt = (RotateClause*)node;

            if (p2walker(stmt->forColName, context))
                return true;
            if (p2walker(stmt->inExprList, context))
                return true;
            if (p2walker(stmt->aggregateFuncCallList, context))
                return true;
        } break;
        case T_UnrotateClause: {
            UnrotateClause *stmt = (UnrotateClause*)node;

            if (p2walker(stmt->forColName, context))
                return true;
            if (p2walker(stmt->inExprList, context))
                return true;
        } break;
        case T_RotateInCell: {
            RotateInCell *stmt = (RotateInCell*)node;

            if (p2walker(stmt->rotateInExpr, context))
                return true;
        } break;
        case T_UnrotateInCell: {
            UnrotateInCell *stmt = (UnrotateInCell*)node;

            if (p2walker(stmt->aliaList, context))
                return true;
            if (p2walker(stmt->unrotateInExpr, context))
                return true;
        } break;
        case T_KeepClause: {
            KeepClause *kp = (KeepClause *) node;

            if (p2walker(kp->keep_order, context))
                return true;
        } break;
        case T_UpsertClause:
            return p2walker(((UpsertClause*)node)->targetList, context);
        case T_CommonTableExpr:
            return p2walker(((CommonTableExpr*)node)->ctequery, context);
        case T_AutoIncrement:
            return p2walker(((AutoIncrement*)node)->expr, context);
        case T_UserVar:
            /* @var do not need recursion */
            break;
        case T_CreateStmt: {
          CreateStmt *stmt = (CreateStmt*)node;

          if (p2walker(stmt->relation, context)) {
            return true;
          }
          if (p2walker(stmt->tableElts, context)) {
              return true;
          }
          if ((void*)create_json_walker == (void*)p2walker) {
            json_walker_context* cxt = (json_walker_context*)context;
            cxt->cur_obj = cxt->pre_obj;
          }
        } break;
        case T_CreateTableAsStmt: {
          CreateTableAsStmt *stmt = (CreateTableAsStmt*)node;

          if (p2walker(stmt->into, context)) {
            return true;
          }
          if (p2walker(stmt->query, context)) {
              return true;
          }
          if ((void*)create_json_walker == (void*)p2walker) {
            json_walker_context* cxt = (json_walker_context*)context;
            cxt->cur_obj = cxt->pre_obj;
          }
        } break;
        case T_CompositeTypeStmt:
            return p2walker(((CompositeTypeStmt*)node)->coldeflist, context);
        case T_AlterTableStmt: {
          AlterTableStmt *stmt = (AlterTableStmt*)node;

          if (p2walker(stmt->relation, context)) {
            return true;
          }
          if (p2walker(stmt->cmds, context)) {
              return true;
          }
          if ((void*)create_json_walker == (void*)p2walker) {
            json_walker_context* cxt = (json_walker_context*)context;
            cxt->cur_obj = cxt->pre_obj;
          }
        } break;
        case T_AlterTableCmd:
            return p2walker(((AlterTableCmd*)node)->def, context);
        case T_TableLikeClause:
            return p2walker(((TableLikeClause*)node)->relation, context);
        case T_DropStmt: {
            DropStmt *stmt = (DropStmt*)node;

            if (p2walker(((DropStmt*)node)->objects, context)) {
                return true;
            }
            if (p2walker(((DropStmt*)node)->arguments, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_TruncateStmt: {
            TruncateStmt *stmt = (TruncateStmt*)node;

            if (p2walker(stmt->relations, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        case T_ViewStmt: {
            ViewStmt *stmt = (ViewStmt*)node;

            if (p2walker(stmt->view, context)) {
                return true;
            }
            if (p2walker(stmt->query, context)) {
                return true;
            }
            if ((void*)create_json_walker == (void*)p2walker) {
              json_walker_context* cxt = (json_walker_context*)context;
              cxt->cur_obj = cxt->pre_obj;
            }
        } break;
        default:
            break;
    }
    return false;
}
))


# SQL Parsing
if runner.compatibility_mode == 'A'
  runner.deep_resolve('raw_parser')
elsif runner.compatibility_mode == 'B'
  runner.deep_resolve('dolphin_raw_parser')
end
runner.deep_resolve('MemoryContextStrdupDebug')
runner.deep_resolve('MemoryContextAllocZeroAlignedDebug')
runner.deep_resolve('MemoryContextAllocZeroDebug')
runner.deep_resolve('enlargeBuffer')
runner.deep_resolve('std_MemoryContextDelete')
runner.deep_resolve('std_MemoryContextDeleteChildren')
runner.deep_resolve('MemoryContextAllocExtendedDebug')
runner.deep_resolve('std_MemoryContextDestroyAtThreadExit')
runner.deep_resolve('std_MemoryContextResetAndDeleteChildren')
runner.deep_resolve('std_MemoryContextReset')
runner.deep_resolve('RemoveTrackMemoryContext')
runner.deep_resolve('MemoryContextInit')
runner.deep_resolve('MemoryContextControlSet')
runner.deep_resolve('AllocSetContextCreate')
runner.deep_resolve('MemoryContextSwitchTo')
runner.deep_resolve('CurrentMemoryContext')
runner.deep_resolve('MemoryContextDelete')
runner.deep_resolve('AllocSetDeleteFreeList')
runner.deep_resolve('AllocSetContextSetMethods')
runner.deep_resolve('palloc0')
runner.deep_resolve('set_sentinel')
runner.deep_resolve('sentinel_ok')
runner.deep_resolve('GenericMemoryAllocator::AllocSetContextSetMethods')
runner.deep_resolve('GenericMemoryAllocator::AllocSetAlloc')
runner.deep_resolve('GenericMemoryAllocator::AllocSetFree')
runner.deep_resolve('GenericMemoryAllocator::AllocSetRealloc')
runner.deep_resolve('GenericMemoryAllocator::AllocSetInit')
runner.deep_resolve('GenericMemoryAllocator::AllocSetReset')
runner.deep_resolve('GenericMemoryAllocator::AllocSetDelete')
runner.deep_resolve('GenericMemoryAllocator::AllocSetGetChunkSpace')
runner.deep_resolve('GenericMemoryAllocator::AllocSetIsEmpty')
runner.deep_resolve('GenericMemoryAllocator::AllocSetStats')
runner.deep_resolve('AlignMemoryAllocator::AllocSetContextCreate')
runner.deep_resolve('repallocDebug')
runner.deep_resolve('repallocHugeDebug')
runner.deep_resolve('RemoveTrackMemoryContext')
runner.deep_resolve('RemoveTrackMemoryInfo')
runner.deep_resolve('MemoryContextGroup::Init')
runner.deep_resolve('hash_estimate_size')
runner.deep_resolve('uint32_hash')
runner.deep_resolve('string_hash')
runner.deep_resolve('tag_hash')
runner.deep_resolve('strlcpy')
runner.deep_resolve('hash_any')
runner.deep_resolve('hash_uint32')
runner.deep_resolve('bms_hash_value')
runner.deep_resolve('GetCurrentTimestamp')
runner.deep_resolve('hash_any')
runner.deep_resolve('buf_hash_operate')
runner.deep_resolve('knl_session_init')
runner.deep_resolve('knl_thread_init')
runner.deep_resolve('DestroyStringInfo')
runner.deep_resolve('create_session_context')
runner.deep_resolve('DllistWithLock::~DllistWithLock')
runner.deep_resolve('DllistWithLock::DllistWithLock')
runner.deep_resolve('DLInitElem')
# force include enlargeBufferHuge/enlargeBuffer
runner.deep_resolve('pg_mbstrlen_with_len')

runner.add_symbols('raw_expression_tree_walker')
#include symbols: ScanKeywords
runner.add_symbols('ScanKeywordCategories')
runner.add_symbols('memory_context_group_name')

# output
runner.write_out
# compress
runner.compress

