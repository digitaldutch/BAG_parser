# Rijksdriehoek conversie routines
# Wikipedia: https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten
# Source: https://github.com/djvanderlaan/rijksdriehoek

# Benaderingsformules zijn gemaakt door F.H. Schreutelkamp en G.L. Strang van Hees
# Document: Benaderingsformules voor de transformatie tussen RD- en WGS84-kaartcoördinaten
# https://nedcad.nl/concept/coordinaat-transformaties/

# These are approximation equations. They are about 28 cm on average (50 cm at most) different from the official transformation
# RDNAPTRANS2018. bag/coordinates.py adds a constant correction and only uses them if pyproj is not installed.

# Locatie van de spits van de Onze Lieve Vrouwetoren ('Lange Jan') in Amersfoort
X0 = 155000
Y0 = 463000
PHI0 = 52.15517440
LAM0 = 5.38720621
pqk = [(0, 1, 3235.65389),
       (2, 0, -32.58297),
       (0, 2, -0.24750),
       (2, 1, -0.84978),
       (0, 3, -0.06550),
       (2, 2, -0.01709),
       (1, 0, -0.00738),
       (4, 0, 0.00530),
       (2, 3, -0.00039),
       (4, 1, 0.00033),
       (1, 1, -0.00012)]

pql = [(1, 0, 5260.52916),
       (1, 1, 105.94684),
       (1, 2, 2.45656),
       (3, 0, -0.81885),
       (1, 3, 0.05594),
       (3, 1, -0.05607),
       (0, 1, 0.01199),
       (3, 2, -0.00256),
       (1, 4, 0.00128),
       (0, 2, 0.00022),
       (2, 0, -0.00022),
       (5, 0, 0.00026)]


def rijksdriehoek_to_wgs84(x, y):
    # Convert rijksdriehoek-coordinates into WGS84 coordinates. Input parameters: x (float), y (float).

    dx = 1E-5 * (x - X0)
    dy = 1E-5 * (y - Y0)

    phi = PHI0
    lam = LAM0

    for p, q, k in pqk:
        phi += k * dx ** p * dy ** q / 3600

    for p, q, l in pql:
        lam += l * dx ** p * dy ** q / 3600

    return [phi, lam]
