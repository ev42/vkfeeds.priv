#!/usr/bin/ruby
#encoding: utf-8

require 'vk-ruby'
require 'json'
require 'set'
require 'redis'
require 'mongo'


class VKFeed
	def initialize
		@vk = VK::Application.new
	end
	
	def newsfeed
		@vk.newsfeed
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
						:extended=>1,
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


	def query(q, opts = {})
		tm = (Time.now.to_i - 900)
		qopts = {:start_time=>tm.to_i, :v=>'5.0', :extended=>1}.merge(opts)
		last_opts = {}
		buf= Hash.new(Set.new)
		until (qopts[:start_id]||1) == 0 do
			resp = fetch_statuses(q, qopts)
			last_qopts = qopts.dup
			resp.each do |k, val|
				next unless val.is_a? Array
				buf[k] |= val
			end
			qopts[:offset]=resp['new_offset']
			qopts[:start_id]=resp['new_from']
		end
		buf
	end

end
