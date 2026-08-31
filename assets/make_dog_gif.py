#!/usr/bin/env python3
"""Build an animated 'YOU GET A DOG!' meme GIF (original cartoon homage)."""
import math
from PIL import Image, ImageDraw, ImageFont

W = H = 600
S = 2                      # supersample factor
CW, CH = W * S, H * S
FONT_PATH = "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"

FLOOR_Y = 474
BAR_Y = 502

SKIN = (158, 104, 68)
SKIN_L = (182, 126, 88)
SKIN_D = (124, 78, 50)
HAIR = (52, 32, 28)
HAIR_H = (92, 60, 48)
COAT = (208, 44, 98)
COAT_D = (168, 28, 78)
TOP = (250, 224, 120)
STAGE_T = (44, 20, 68)
STAGE_B = (108, 44, 120)
FLOOR = (66, 28, 82)
BAR = (16, 11, 20)

DOG_COLORS = [
    ((214, 160, 96), (168, 116, 62)),
    ((238, 214, 178), (196, 166, 128)),
    ((124, 100, 88), (88, 70, 60)),
    ((236, 176, 116), (190, 132, 78)),
    ((180, 180, 188), (136, 136, 146)),
]
CONFETTI = [(255, 209, 74), (255, 106, 158), (109, 214, 255), (150, 255, 176), (255, 148, 92)]


def s(v):
    return v * S


def rot(pts, ang, cx, cy):
    ca, sa = math.cos(ang), math.sin(ang)
    return [(cx + (x - cx) * ca - (y - cy) * sa, cy + (x - cx) * sa + (y - cy) * ca) for x, y in pts]


def ell(d, cx, cy, rx, ry, fill):
    d.ellipse([s(cx - rx), s(cy - ry), s(cx + rx), s(cy + ry)], fill=fill)


def line(d, pts, fill, width):
    d.line([(s(x), s(y)) for x, y in pts], fill=fill, width=s(width), joint="curve")


def poly(d, pts, fill):
    d.polygon([(s(x), s(y)) for x, y in pts], fill=fill)


# ---------------------------------------------------------------- background
def draw_stage(d, t, flash):
    for i in range(0, H, 2):
        k = i / H
        c = tuple(int(STAGE_T[j] + (STAGE_B[j] - STAGE_T[j]) * (k ** 0.85)) for j in range(3))
        d.rectangle([0, s(i), CW, s(i + 2)], fill=c)
    for i in range(12):
        a = -math.pi / 2 + (i - 5.5) * 0.30 + math.sin(t * 2.0) * 0.05
        if i % 2 == 0:
            poly(d, [(300, 230),
                     (300 + math.cos(a - 0.06) * 900, 230 + math.sin(a - 0.06) * 900),
                     (300 + math.cos(a + 0.06) * 900, 230 + math.sin(a + 0.06) * 900)],
                 (STAGE_B[0] + 14, STAGE_B[1] + 9, STAGE_B[2] + 13))
    for i in range(56, 0, -1):
        k = i / 56
        a = 6 + 26 * (1 - k) ** 1.7 + flash
        ell(d, 300, 280, 70 + k * 310, 66 + k * 290,
            (min(255, int(STAGE_B[0] + a + 20)), min(255, int(STAGE_B[1] + a + 8)),
             min(255, int(STAGE_B[2] + a + 14))))
    d.rectangle([0, s(FLOOR_Y), CW, CH], fill=FLOOR)
    d.rectangle([0, s(FLOOR_Y), CW, s(FLOOR_Y + 5)], fill=(150, 66, 158))


# ---------------------------------------------------------------- the dog
def draw_dog(d, x, y, sc, ci, phase=0.0):
    if sc <= 0.02:
        return
    body, shade = DOG_COLORS[ci % len(DOG_COLORS)]
    light = tuple(min(255, c + 26) for c in body)

    def P(px, py):
        return (x + px * sc, y + py * sc)

    def E(px, py, rx, ry, fill):
        ell(d, x + px * sc, y + py * sc, rx * sc, ry * sc, fill)

    ta = -0.9 + math.sin(phase * 6) * 0.7
    line(d, [P(19, 4), P(19 + math.cos(ta) * 21, 4 + math.sin(ta) * 21)], shade, max(1, int(5 * sc)))
    E(0, 8, 22, 18, body)
    E(-13, 22, 7.5, 6, shade)
    E(13, 22, 7.5, 6, shade)
    hx, hy = -2, -18
    E(hx, hy, 20, 18, body)
    for sgn in (-1, 1):
        poly(d, [P(hx - sgn * 16, hy - 10), P(hx - sgn * 26, hy + 1),
                 P(hx - sgn * 21, hy + 17), P(hx - sgn * 11, hy + 4)], shade)
    E(hx, hy + 8, 12, 9, light)
    E(hx, hy + 3, 4.6, 3.6, (38, 28, 30))
    E(hx, hy + 14, 3.6, 4.6, (240, 116, 140))
    for sgn in (-1, 1):
        E(hx + sgn * 8, hy - 4, 3.4, 3.8, (30, 24, 26))
        E(hx + sgn * 8 + 1.2, hy - 5.6, 1.3, 1.3, (255, 255, 255))


# ---------------------------------------------------------------- the host
def draw_arm(d, sh, hand, out, point):
    mx, my = (sh[0] + hand[0]) / 2, (sh[1] + hand[1]) / 2
    elb = (mx + out * 15, my + 12)
    line(d, [sh, elb, hand], COAT_D, 27)
    line(d, [sh, elb, hand], COAT, 21)
    ell(d, hand[0], hand[1], 15, 15, SKIN)
    if point:
        px, py = point
        n = math.hypot(px, py) or 1
        px, py = px / n, py / n
        line(d, [(hand[0] + px * 3, hand[1] + py * 3), (hand[0] + px * 24, hand[1] + py * 24)], SKIN, 9)
        ell(d, hand[0] + px * 24, hand[1] + py * 24, 4.5, 4.5, SKIN_L)
    ell(d, hand[0] - 4, hand[1] - 4, 6, 6, SKIN_L)


def draw_host(d, pose, bob, mouth, blink):
    cy = bob
    shl, shr = (242, 332 + cy), (358, 332 + cy)
    (lhx, lhy), (rhx, rhy) = pose["lh"], pose["rh"]
    lh, rh = (lhx, lhy + cy), (rhx, rhy + cy)
    HY = 236 + cy                                   # face centre

    # torso
    poly(d, [(238, 322 + cy), (362, 322 + cy), (396, FLOOR_Y), (204, FLOOR_Y)], COAT)
    poly(d, [(238, 322 + cy), (300, 322 + cy), (300, FLOOR_Y), (204, FLOOR_Y)], COAT_D)
    poly(d, [(270, 320 + cy), (300, 392 + cy), (330, 320 + cy)], TOP)
    poly(d, [(268, 320 + cy), (300, 390 + cy), (284, 322 + cy)], (234, 76, 124))
    poly(d, [(332, 320 + cy), (300, 390 + cy), (316, 322 + cy)], (234, 76, 124))
    ell(d, 300, 322 + cy, 62, 15, COAT)
    ell(d, 300, 300 + cy, 19, 20, SKIN_D)

    # arms, in front of the coat
    draw_arm(d, shr, rh, 1, pose.get("rp"))
    draw_arm(d, shl, lh, -1, pose.get("lp"))

    # head
    ell(d, 300, HY - 10, 76, 74, HAIR)
    ell(d, 256, HY + 26, 33, 40, HAIR)
    ell(d, 344, HY + 26, 33, 40, HAIR)
    ell(d, 300, HY, 52, 57, SKIN)
    ell(d, 300, HY - 44, 55, 30, HAIR)              # fringe, clear of the eyes
    ell(d, 268, HY - 52, 27, 21, HAIR_H)
    ell(d, 332, HY - 50, 23, 18, HAIR_H)
    ell(d, 300, HY - 66, 40, 20, HAIR_H)

    for sgn in (-1, 1):
        line(d, [(300 + sgn * 33, HY - 12), (300 + sgn * 12, HY - 18)], HAIR, 6)
        ex = 300 + sgn * 21
        if blink:
            line(d, [(ex - 9, HY - 1), (ex + 9, HY - 1)], (42, 30, 28), 4)
        else:
            ell(d, ex, HY - 1, 11, 9, (252, 250, 246))
            ell(d, ex + sgn, HY, 5.2, 5.6, (44, 30, 26))
            ell(d, ex + sgn - 1.6, HY - 2, 1.8, 1.8, (255, 255, 255))
    ell(d, 262, HY + 20, 12, 8, (192, 102, 98))
    ell(d, 338, HY + 20, 12, 8, (192, 102, 98))

    # wide open grin
    my = HY + 32
    mh = 9 + mouth * 15
    ell(d, 300, my, 25, mh, (78, 26, 36))
    ell(d, 300, my - mh * 0.66, 22, mh * 0.34, (255, 253, 248))
    ell(d, 300, my + mh * 0.52, 9, mh * 0.32, (228, 100, 122))
    ell(d, 240, HY + 22, 7, 9, (252, 214, 96))
    ell(d, 360, HY + 22, 7, 9, (252, 214, 96))


# ---------------------------------------------------------------- caption
def draw_caption(img, text, scale=1.0):
    d = ImageDraw.Draw(img)
    d.rectangle([0, s(BAR_Y), CW, CH], fill=BAR)
    size = int(s(48) * scale)
    while size > s(20):
        font = ImageFont.truetype(FONT_PATH, size)
        if d.textlength(text, font=font) <= CW - s(40):
            break
        size -= s(1)
    font = ImageFont.truetype(FONT_PATH, size)
    d.text((CW // 2, s((BAR_Y + H) // 2)), text, font=font, fill=(255, 255, 255),
           anchor="mm", stroke_width=int(s(4)), stroke_fill=(12, 8, 16))


# ---------------------------------------------------------------- frames
POSE_L = {"lh": (162, 380), "rh": (412, 396), "lp": (-1, 0.42)}
POSE_R = {"lh": (188, 396), "rh": (438, 380), "rp": (1, 0.42)}
POSE_C = {"lh": (192, 424), "rh": (408, 424), "lp": (-0.3, 1), "rp": (0.3, 1)}
POSE_UP = {"lh": (148, 168), "rh": (452, 168)}

# (pose, caption, dogs [(x, y, scale, colour, pop-frame)], finale)
BEATS = [
    (POSE_L, "YOU GET A DOG!", [(96, 440, 1.5, 0, 1)], False),
    (POSE_R, "AND YOU GET A DOG!", [(96, 440, 1.5, 0, -9), (504, 440, 1.5, 1, 1)], False),
    (POSE_C, "AND YOU GET A DOG!", [(96, 440, 1.5, 0, -9), (504, 440, 1.5, 1, -9),
                                    (300, 452, 1.7, 3, 1)], False),
    (POSE_UP, "EVERYBODY GETS A DOG!", [(96, 440, 1.5, 0, -9), (504, 440, 1.5, 1, -9),
                                        (300, 452, 1.7, 3, -9), (196, 446, 1.3, 2, 1),
                                        (404, 446, 1.3, 4, 2), (40, 452, 1.2, 3, 3),
                                        (560, 452, 1.2, 1, 4)], True),
]
BEAT_LEN = [7, 7, 7, 14]

frames, durations = [], []
gf = 0
for (pose, cap, dogs, finale), nlen in zip(BEATS, BEAT_LEN):
    for f in range(nlen):
        t = gf * 0.1
        img = Image.new("RGB", (CW, CH))
        d = ImageDraw.Draw(img)
        draw_stage(d, t, 14 if (finale and f % 2 == 0) else 0)

        if finale:
            for i in range(34):
                cx = (i * 137) % 600
                cy = (((i * 91) % 600) + f * 26) % 660 - 40
                col = CONFETTI[i % len(CONFETTI)]
                w_ = 7 + (i % 3) * 3
                if i % 2:
                    ell(d, cx + math.sin(t * 4 + i) * 8, cy, w_ * 0.5, w_ * 0.5, col)
                else:
                    poly(d, rot([(cx - w_, cy - 4), (cx + w_, cy - 4),
                                 (cx + w_, cy + 4), (cx - w_, cy + 4)], t * 3 + i, cx, cy), col)

        bob = math.sin(t * 7) * 4 + (math.sin(t * 22) * 3 if finale else 0)
        draw_host(d, pose, bob, 0.5 + 0.5 * abs(math.sin(t * 8)), blink=(gf % 17 == 8))

        for (dx, dy, dsc, dci, start) in dogs:
            k = f - start
            if k < 0:
                continue
            pop = 1.0 if k >= 3 else [0.35, 1.22, 1.06][k]
            hop = -abs(math.sin(t * 8 + dx)) * (8 if finale else 3)
            draw_dog(d, dx, dy + hop, dsc * pop, dci, t + dx)

        draw_caption(img, cap, 1.0 + (0.06 * math.sin(gf * 1.6) if finale else 0))
        frames.append(img.resize((W, H), Image.LANCZOS))
        durations.append(150 if (f == nlen - 1 and not finale) else (90 if finale else 110))
        gf += 1

frames.append(frames[-1])
durations.append(700)

# one shared palette keeps colours stable across frames
cols = len(frames) // 2 + 1
master = Image.new("RGB", (W * 2, H * cols))
for i, fr in enumerate(frames):
    master.paste(fr, ((i % 2) * W, (i // 2) * H))
pal = master.quantize(colors=128, method=Image.MEDIANCUT)
pframes = [fr.quantize(palette=pal, dither=Image.NONE) for fr in frames]

out = "assets/you-get-a-dog.gif"
pframes[0].save(out, save_all=True, append_images=pframes[1:], duration=durations,
                loop=0, optimize=True, disposal=2)
print("wrote", out, len(pframes), "frames")
