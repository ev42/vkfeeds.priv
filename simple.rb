#!/usr/bin/ruby
#encoding: utf-8

require 'vk-ruby'
require 'json'
require 'set'
require 'redis'
require 'benchmark'

time = Benchmark.measure do
	def vk
		$vk = VK::Application.new unless $vk
		$vk
	end

	def newsfeed
		VK::Application.new.newsfeed
	end

	def fetch_statuses_num(word, opts={})
		newsfeed.search( { :q => word,
						:extended=>0,
						:count => 1
		}.merge(opts)
					   )['count']
	end

	def fetch_statuses(word, opts={})
		newsfeed.search( { :q => word,
						:extended=>0,
						:count => 199
		}.merge(opts)
					   )
	end

	def htags_extract(items)
		htags = Set.new
		items.map{|i| htags|=i['text'].scan(/\#[a-zA-Zа-яА-Я0-9_]+/) }
			htags.to_a		
	end

	def redis_store(bkey, shash)
		rd = Redis.new
		# w:bkey:items_type = Set.new(items)
		#
		puts '# updating data in redis for word %s'%bkey	
		shash.each do |what, items|
			sname = 'w:%s:%s'%[bkey,what]

			puts 'Updating set %s (size %d) with %d items'%[sname, rd.scard(sname), items.size]
			items.each{|i| rd.sadd  sname, i.to_json}
			puts 'Updated set %s. Now size %d'%[sname, rd.scard(sname)]
		end
		puts '# %d data groups stored'%shash.size
	end

	words =  IO.read(ARGV[0]).split("\n")
	vk = VK::Application.new :logger => nil
	vk.adapter = :em_http
	items_all = Set.new
	$redis = Redis.new

			buf= Hash.new(Set.new)
			words << '' if words.empty?
			puts 'Words num: %d'%words.size
			tm = (Time.now.to_i - 900)
			qopts = {:start_time=>tm.to_i, :v=>'5.0', :extended=>1}
			last_opts = {}
			until (qopts[:start_id]||1) == 0 do
				vk.in_parallel do
					words.each do |word|
					resp = fetch_statuses(word, qopts)
					resp.each do |k, val|
						next unless val.is_a? Array
						buf[k] |= val
					end
					end
				end
				last_qopts = qopts.dup
				#File.open('newsfeed/%s.%d'%[word,tm],'w'){|f| f.write buf.to_json}
				qopts[:offset]=resp['new_offset']
				qopts[:start_id]=resp['new_from']
			end
			items_all |= buf['items']	
			puts 'Done %d requests. Pushing data to redis'%buf.size
		end

	puts 'unique items: %s'%items_all.size

puts 'Total time: %s'%time
