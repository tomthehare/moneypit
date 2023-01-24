import colorsys
import logging

class Color:

    RED = 'FF0000'
    WHITE = 'FFFFFF'

    def __init__(self, hex):
        self.red = hex[0:2]
        self.green = hex[2:4]
        self.blue = hex[4::]
        self.hue = 0
        self.saturation = 0
        self.lightness = 0

        self.set_hsl()

    def convert_hex_to_float(self, hexval):
        return float(int(hexval, 16) / 255)

    def get_rgb_string(self):
        return self.red + self.green + self.blue

    def set_lightness(self, lightness):
        self.lightness = lightness
        self.set_rgb() # recompute based on new lightness

    def set_rgb(self):
        (red, green, blue) = colorsys.hls_to_rgb(self.hue, self.lightness, self.saturation)

        self.red = hex(int(255 * red)).replace('0x', '')
        self.green = hex(int(255 * green)).replace('0x', '')
        self.blue = hex(int(255 * blue)).replace('0x', '')

        if len(self.red) == 1:
            self.red = '0' + self.red

        if len(self.green) == 1:
            self.green = '0' + self.green

        if len(self.blue) == 1:
            self.blue = '0' + self.blue

    def set_hsl(self):
        (self.hue, self.lightness, self.saturation) = colorsys.rgb_to_hls(
            self.convert_hex_to_float(self.red),
            self.convert_hex_to_float(self.green),
            self.convert_hex_to_float(self.blue)
        )