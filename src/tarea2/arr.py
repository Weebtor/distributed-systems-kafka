dicc1 = {'uno': 1, 'dos': 2, 'tres': 3, 'cuatro':4}
dicc2 = {'cinco': 5, 'seis': 6}

#dicc1.setdefault('cinco', 5)
ix = 1
for key in dicc1:
    if (key == 'ocho'):
        dicc1[key] = dicc1[key] + 8
        break
else:
    for key in dicc1:
        dicc2.setdefault(key, dicc1[key])
print(dicc1)
print(dicc2)

