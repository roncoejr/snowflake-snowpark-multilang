def cutInHalf ( x ):
    return ((int(x)/2))

def rateDistance( x ):
    statement = ""
    nRes = float(x)

    if (nRes <= 5.0):
        statement = "Short Distance"

    if ((nRes > 5.0) and (nRes <= 15.0)):
        statement = "Medium Distance"

    if (nRes > 15):
        statement = "That's a long way!"

    return statement
