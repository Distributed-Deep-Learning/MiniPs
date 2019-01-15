import time

print "Start Looping..."
counter = 0
while True:
    time.sleep(2)
    counter += 1
    print "Looping With Count=" + str(counter)
    if counter >= 500:
        break

print "Looping Done"