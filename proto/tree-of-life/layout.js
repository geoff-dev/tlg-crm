// Deterministic placement for the Tree of Life prototype.
// Leaves and blossoms are never dropped: when the region fills, radii shrink
// and the gap shrinks, then a last pass allows overlap inside the polygon.

export const STONE = { w: 48, h: 56 }

export function hashString(value) {
  const s = String(value)
  let h = 2166136261
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

export function mulberry32(seed) {
  let a = seed >>> 0
  return function rng() {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n))
}

export function pointInPolygon(x, y, poly) {
  let inside = false
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const xi = poly[i][0]
    const yi = poly[i][1]
    const xj = poly[j][0]
    const yj = poly[j][1]
    const intersect = (yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi
    if (intersect) inside = !inside
  }
  return inside
}

export function bounds(poly) {
  let minX = Infinity
  let minY = Infinity
  let maxX = -Infinity
  let maxY = -Infinity
  for (const [x, y] of poly) {
    if (x < minX) minX = x
    if (y < minY) minY = y
    if (x > maxX) maxX = x
    if (y > maxY) maxY = y
  }
  return { minX, minY, maxX, maxY }
}

export function leafRadius(amount) {
  const n = Number(amount)
  const amt = Number.isFinite(n) && n > 0 ? n : 5000
  return clamp(Math.sqrt(amt) / 26, 3.4, 16)
}

export function blossomRadius(changeOrder) {
  const n = Number(changeOrder.estimate_amount ?? changeOrder.sale_amount)
  const amt = Number.isFinite(n) && n > 0 ? n : 3000
  return clamp(Math.sqrt(amt) / 38, 4.2, 9.5)
}

function sample(poly, rng, fallback) {
  const b = bounds(poly)
  const w = b.maxX - b.minX
  const h = b.maxY - b.minY
  for (let i = 0; i < 48; i++) {
    const x = b.minX + rng() * w
    const y = b.minY + rng() * h
    if (pointInPolygon(x, y, poly)) return [x, y]
  }
  if (fallback && pointInPolygon(fallback[0], fallback[1], poly)) return fallback.slice()
  return [poly[0][0], poly[0][1]]
}

function avoided(x, y, polygons) {
  if (!polygons) return false
  for (const poly of polygons) {
    if (poly && pointInPolygon(x, y, poly)) return true
  }
  return false
}

function tryPlace(items, poly, scale, gap, avoid, fallback) {
  const placed = []
  const grid = new Map()
  const cell = 26
  const keyOf = (x, y) => Math.floor(x / cell) + "," + Math.floor(y / cell)
  const neighbors = (x, y, reach) => {
    const cx = Math.floor(x / cell)
    const cy = Math.floor(y / cell)
    const steps = Math.max(1, Math.ceil(reach / cell))
    const out = []
    for (let iy = cy - steps; iy <= cy + steps; iy++) {
      for (let ix = cx - steps; ix <= cx + steps; ix++) {
        const bucket = grid.get(ix + "," + iy)
        if (bucket) out.push(...bucket)
      }
    }
    return out
  }

  for (const item of items) {
    const r = Math.max(1.6, item.baseR * scale)
    const rng = mulberry32(hashString(item.id))
    let chosen = null
    const reach = r + 20
    for (let attempt = 0; attempt < 64; attempt++) {
      const pt = sample(poly, rng, fallback)
      if (avoided(pt[0], pt[1], avoid)) continue
      let ok = true
      const near = neighbors(pt[0], pt[1], reach)
      for (const other of near) {
        const dx = other.x - pt[0]
        const dy = other.y - pt[1]
        const minD = (other.r + r) * gap
        if (dx * dx + dy * dy < minD * minD) {
          ok = false
          break
        }
      }
      if (ok) {
        chosen = pt
        break
      }
    }
    if (!chosen) return null
    const rec = { ...item, x: chosen[0], y: chosen[1], r }
    placed.push(rec)
    const key = keyOf(rec.x, rec.y)
    let bucket = grid.get(key)
    if (!bucket) {
      bucket = []
      grid.set(key, bucket)
    }
    bucket.push(rec)
  }
  return placed
}

function forcePlace(items, poly, scale, avoid, fallback) {
  const placed = []
  for (const item of items) {
    const r = Math.max(1.6, item.baseR * scale)
    const rng = mulberry32(hashString("force:" + item.id))
    let chosen = null
    for (let attempt = 0; attempt < 80; attempt++) {
      const pt = sample(poly, rng, fallback)
      if (!avoided(pt[0], pt[1], avoid)) {
        chosen = pt
        break
      }
    }
    if (!chosen) chosen = sample(poly, rng, fallback)
    placed.push({ ...item, x: chosen[0], y: chosen[1], r })
  }
  return placed
}

export function placeMarks(items, poly, options = {}) {
  const avoid = options.avoid || []
  const fallback = options.fallback || null
  const startGap = options.gap ?? 0.9
  const ordered = items.slice().sort((a, b) => b.baseR - a.baseR || String(a.id).localeCompare(String(b.id)))
  let scale = 1
  let gap = startGap
  for (let round = 0; round < 22; round++) {
    const placed = tryPlace(ordered, poly, scale, gap, avoid, fallback)
    if (placed && placed.length === ordered.length) return placed
    if (round < 14) scale *= 0.84
    else gap *= 0.72
  }
  return forcePlace(ordered, poly, Math.max(scale, 0.18), avoid, fallback)
}

function lostSort(a, b) {
  const da = a.date_lost || ""
  const db = b.date_lost || ""
  if (da !== db) {
    if (!da) return 1
    if (!db) return -1
    return db.localeCompare(da)
  }
  return String(a.id).localeCompare(String(b.id))
}

export function stoneCorners(stone) {
  const w = STONE.w * stone.scale
  const h = STONE.h * stone.scale
  return [
    [stone.x - w / 2, stone.y],
    [stone.x + w / 2, stone.y],
    [stone.x + w / 2, stone.y - h],
    [stone.x - w / 2, stone.y - h],
  ]
}

export function layoutScene(data, anchors) {
  const leafItems = data.sold.map((project) => {
    const hueHash = hashString(project.id)
    return {
      id: project.id,
      kind: "leaf",
      baseR: leafRadius(project.sale_amount),
      hue: 98 + (hueHash % 34),
      rot: (hashString(project.id + ":rot") % 80) - 40,
      project,
    }
  })
  const leaves = placeMarks(leafItems, anchors.canopy, {
    gap: 0.9,
    fallback: anchors.canopyInterior,
  })

  const blossomItems = data.changeOrders.map((changeOrder) => ({
    id: changeOrder.id,
    kind: "blossom",
    baseR: blossomRadius(changeOrder),
    rot: (hashString(changeOrder.id + ":rot") % 50) - 25,
    changeOrder,
  }))
  const blossoms = placeMarks(blossomItems, anchors.blossomBed, {
    gap: 0.72,
    avoid: anchors.trunk ? [anchors.trunk] : [],
    fallback: anchors.blossomInterior,
  })

  const slots = anchors.stones
  const lost = data.lost.slice().sort(lostSort)
  const headstones = lost.map((project, index) => {
    const slot = slots[index % slots.length]
    return {
      id: project.id,
      kind: "headstone",
      project,
      page: Math.floor(index / slots.length),
      slot: index % slots.length,
      x: slot.x,
      y: slot.y,
      scale: slot.scale,
    }
  })

  return {
    leaves,
    blossoms,
    headstones,
    pageSize: slots.length,
    pageCount: Math.max(1, Math.ceil(lost.length / Math.max(slots.length, 1))),
  }
}

export function checkLayout(layout, data, anchors) {
  const errors = []
  if (layout.leaves.length !== data.sold.length) {
    errors.push(`leaves ${layout.leaves.length} != sold ${data.sold.length}`)
  }
  if (layout.blossoms.length !== data.changeOrders.length) {
    errors.push(`blossoms ${layout.blossoms.length} != change orders ${data.changeOrders.length}`)
  }
  if (layout.headstones.length !== data.lost.length) {
    errors.push(`headstones ${layout.headstones.length} != lost ${data.lost.length}`)
  }
  const seen = new Set()
  for (const leaf of layout.leaves) {
    seen.add(leaf.id)
    if (!pointInPolygon(leaf.x, leaf.y, anchors.canopy)) errors.push(`leaf outside canopy ${leaf.id}`)
  }
  if (seen.size !== layout.leaves.length) errors.push("duplicate leaf ids")
  for (const blossom of layout.blossoms) {
    if (!pointInPolygon(blossom.x, blossom.y, anchors.blossomBed)) {
      errors.push(`blossom outside bed ${blossom.id}`)
    }
    if (anchors.trunk && pointInPolygon(blossom.x, blossom.y, anchors.trunk)) {
      errors.push(`blossom on trunk ${blossom.id}`)
    }
  }
  for (const stone of layout.headstones) {
    for (const [x, y] of stoneCorners(stone)) {
      if (!pointInPolygon(x, y, anchors.cemetery)) {
        errors.push(`headstone outside cemetery ${stone.id}`)
        break
      }
    }
  }
  const pages = new Set(layout.headstones.map((stone) => stone.page))
  if (layout.headstones.length && pages.size !== layout.pageCount) {
    errors.push("page count does not cover every headstone")
  }
  return errors
}

export function layoutSignature(layout) {
  const bits = layout.leaves.map((leaf) => `${leaf.id}@${leaf.x.toFixed(2)},${leaf.y.toFixed(2)}`)
  bits.push(...layout.blossoms.map((mark) => `${mark.id}@${mark.x.toFixed(2)},${mark.y.toFixed(2)}`))
  return hashString(bits.join("|")).toString(16)
}
