require 'aws-sdk'

s3 = Aws::S3::Resource.new(region: 'us-east-1')
bucket = s3.bucket('ensemblejs')

events = bucket.objects(prefix: 'events/').map do |obj|
  JSON.parse(bucket.object(obj.key).get.body.string)
end

events.each do |event|
  puts event
end