import { useEffect, useMemo, useRef } from 'react'
import { useColorScheme } from '@mui/material'
import createGlobe from 'cobe'

export interface FlightRoute {
  from: [number, number]
  to: [number, number]
  color: [number, number, number]
  id?: string
}

interface GlobeProps {
  routes?: FlightRoute[]
  width?: number
  height?: number
}

type RGB = [number, number, number]

// Always a dark globe — it's a data viz, not a classroom globe.
// Only the atmospheric glow tint shifts between modes.
const SCHEMES = {
  dark: {
    glowColor: [0.08, 0.22, 0.8] as RGB,
  },
  light: {
    glowColor: [0.14, 0.3, 0.9] as RGB,
  },
}

const BASE: {
  dark: 1
  baseColor: RGB
  markerColor: RGB
  diffuse: number
  mapBrightness: number
  mapBaseBrightness: number
} = {
  dark: 1,
  baseColor: [0.038, 0.038, 0.044],
  markerColor: [0.49, 0.659, 1.0],
  diffuse: 0.52,
  mapBrightness: 7.5,
  mapBaseBrightness: 0.012,
}

export const Globe = ({ routes = [], width = 600, height = 600 }: GlobeProps) => {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const { colorScheme } = useColorScheme()
  const glowColor = SCHEMES[colorScheme === 'light' ? 'light' : 'dark'].glowColor

  const markers = useMemo(() =>
    Array.from(
      new Map(
        routes.flatMap(({ from, to, color }) => [
          [`${from[0]},${from[1]}`, { location: from, size: 0.04, color }],
          [`${to[0]},${to[1]}`, { location: to, size: 0.04, color }],
        ])
      ).values()
    ),
    [routes]
  )

  useEffect(() => {
    if (!canvasRef.current) return

    // Start facing the Atlantic so US routes are immediately visible
    let phi = 4.8
    let animFrameId: number

    const globe = createGlobe(canvasRef.current, {
      devicePixelRatio: 2,
      width: width * 2,
      height: height * 2,
      phi,
      theta: 0.2,
      ...BASE,
      glowColor,
      mapSamples: 45000,
      markers,
      arcs: routes.map(({ from, to, color, id }) => ({ from, to, color, id })),
      arcColor: glowColor,
      arcWidth: 1.4,
    })

    const animate = () => {
      phi += 0.003
      globe.update({ phi })
      animFrameId = requestAnimationFrame(animate)
    }

    animFrameId = requestAnimationFrame(animate)

    return () => {
      cancelAnimationFrame(animFrameId)
      globe.destroy()
    }
  }, [routes, markers, width, height, glowColor])

  return (
    <canvas
      ref={canvasRef}
      style={{ width, height, display: 'block' }}
    />
  )
}
