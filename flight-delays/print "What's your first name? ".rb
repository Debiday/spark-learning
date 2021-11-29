print "What's your first name? "
first_name = gets.chomp
first_name.capitalize!

print "What's your last name? "
last_name = gets.chomp
last_name.capitalize!

print "What city are you from? "
city = gets.chomp
city.capitalize!

print "What state or province are you from? "
state = gets.chomp
state.upcase!

puts "Your name is #{first_name} #{last_name} and you're from #{city}, #{state}!"

# _______________________________________________________
# Daffy Duck
# _______________________________________________________
print "Thtring, pleathe!: "
user_input = gets.chomp
user_input.downcase!

if user_input.include? "s"
    user_input.gsub!(/s/, "th")
else
    puts "Nothing to do here!"
end

puts "Your string is: #{user_input}"

# _______________________________________________________
# Until
# _______________________________________________________
counter = 1
until counter == 11
    puts counter
    # Add code to update 'counter' here!
    counter = counter + 1
end

# _______________________________________________________
# Loop do
# _______________________________________________________
i = 20
loop do
    i -= 1
    print "#{i}"
    break if i <= 0
end

# _______________________________________________________
# Next if
# _______________________________________________________
i = 20
loop do
    i -= 1
    next if i % 2 != 0
    print "#{i}"
    break if i <= 0
end
