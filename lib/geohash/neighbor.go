package geohash

import "math"

const (
	dr          = math.Pi / 180.0
	earthRadius = 6372797.560856
	mercatorMax = 20037726.37 // pi * earthRadius
	mercatorMin = -20037726.37
)

func degRad(ang float64) float64 {
	return ang * dr
}

func radDeg(ang float64) float64 {
	return ang / dr
}

func estimatePrecisionByRadius(radiusMeters float64, latitude float64) uint {
	if radiusMeters == 0 {
		return defaultBitSize - 1
	}
	var precision uint = 1
	for radiusMeters < mercatorMax {
		radiusMeters *= 2
		precision++
	}
	/* Make sure range is included in most of the base cases. */
	precision -= 2
	if latitude > 66 || latitude < -66 {
		precision--
		if latitude > 80 || latitude < -80 {
			precision--
		}
	}
	if precision < 1 {
		precision = 1
	}
	if precision > 32 {
		precision = 32
	}
	return precision*2 - 1
}

// Distance computes the distance between two given coordinates in meter
func Distance(latitude1, longitude1, latitude2, longitude2 float64) float64 {
	radLat1 := degRad(latitude1)
	radLat2 := degRad(latitude2)
	a := radLat1 - radLat2
	b := degRad(longitude1) - degRad(longitude2)
	return 2 * earthRadius * math.Asin(math.Sqrt(math.Pow(math.Sin(a/2), 2)+
		math.Cos(radLat1)*math.Cos(radLat2)*math.Pow(math.Sin(b/2), 2)))
}

// toRange covert geohash prefix to uint64 range
func toRange(scope []byte, precision uint) [2]uint64 {
	lower := ToInt(scope)
	radius := uint64(1 << (64 - precision))
	upper := lower + radius
	return [2]uint64{lower, upper}
}

func ensureValidLat(lat float64) float64 {
	if lat > 90 {
		return 90
	}
	if lat < -90 {
		return -90
	}
	return lat
}

func ensureValidLng(lng float64) float64 {
	if lng > 180 {
		return -360 + lng
	}
	if lng < -180 {
		return 360 + lng
	}
	return lng
}

// GetNeighbours returns geohash code of blocks within radiusMeters to the given coordinate
func GetNeighbours(latitude, longitude, radiusMeters float64) [][2]uint64 {
	precision := estimatePrecisionByRadius(radiusMeters, latitude)

	center, box := encode0(latitude, longitude, precision)
	height := box[0][1] - box[0][0]
	width := box[1][1] - box[1][0]
	centerLng := (box[0][1] + box[0][0]) / 2
	centerLat := (box[1][1] + box[1][0]) / 2
	maxLat := ensureValidLat(centerLat + height)
	minLat := ensureValidLat(centerLat - height)
	maxLng := ensureValidLng(centerLng + width)
	minLng := ensureValidLng(centerLng - width)

	var result [10][2]uint64
	leftUpper, _ := encode0(maxLat, minLng, precision)
	result[1] = toRange(leftUpper, precision)
	upper, _ := encode0(maxLat, centerLng, precision)
	result[2] = toRange(upper, precision)
	rightUpper, _ := encode0(maxLat, maxLng, precision)
	result[3] = toRange(rightUpper, precision)
	left, _ := encode0(centerLat, minLng, precision)
	result[4] = toRange(left, precision)
	result[5] = toRange(center, precision)
	right, _ := encode0(centerLat, maxLng, precision)
	result[6] = toRange(right, precision)
	leftDown, _ := encode0(minLat, minLng, precision)
	result[7] = toRange(leftDown, precision)
	down, _ := encode0(minLat, centerLng, precision)
	result[8] = toRange(down, precision)
	rightDown, _ := encode0(minLat, maxLng, precision)
	result[9] = toRange(rightDown, precision)

	return result[1:]
}
