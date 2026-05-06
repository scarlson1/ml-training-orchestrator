const _NONCONTINENTAL_ICAO_TO_IATA: Record<string, string> = {
  ANC: 'PANC',
  FAI: 'PAFA',
  JNU: 'PAJN',
  BET: 'PABT',
  KDK: 'PADQ',
  FBK: 'PAFB',
  AKN: 'PAKN',
  OME: 'PAOM',
  HOM: 'PAHO',
  ADK: 'PADK',
  HNL: 'PHNL',
  OGG: 'PHOG',
  KOA: 'PHKO',
  ITO: 'PHTO',
  SJU: 'TJSJ',
  BQN: 'TJBQ',
  PSE: 'TJPS',
  GUM: 'PGUM',
};

export const iataToIcao = (iata: string): string => {
  if (Object.keys(_NONCONTINENTAL_ICAO_TO_IATA).includes(iata))
    return _NONCONTINENTAL_ICAO_TO_IATA[iata];
  return `K${iata}`;
};

const CARRIER_ABRV_MAP: Record<string, string> = {
  AA: 'American Airlines',
  DL: 'Delta Air Lines',
  UA: 'United Airlines',
  WN: 'Southwest Airlines',
  AS: 'Alaska Airlines',
  B6: 'JetBlue Airways',
  NK: 'Spirit Airlines',
  F9: 'Frontier Airlines',
  HA: 'Hawaiian Airlines',
  G4: 'Allegiant Air',

  // Major International Carriers
  AC: 'Air Canada',
  AF: 'Air France',
  BA: 'British Airways',
  CX: 'Cathay Pacific',
  EK: 'Emirates',
  ET: 'Ethiopian Airlines',
  IB: 'Iberia',
  JL: 'Japan Airlines',
  KL: 'KLM',
  LH: 'Lufthansa',
  QF: 'Qantas',
  QR: 'Qatar Airways',
  SQ: 'Singapore Airlines',
  TK: 'Turkish Airlines',
  VS: 'Virgin Atlantic',
  LX: 'Swiss International Air Lines',
  EI: 'Aer Lingus',
  NZ: 'Air New Zealand',
  NH: 'All Nippon Airways',
  OZ: 'Asiana Airlines',
  OS: 'Austrian Airlines',
  AV: 'Avianca',
  EY: '联合航空 (Etihad Airways)',
  BR: 'EVA Air',
  AY: 'Finnair',
  KE: 'Korean Air',
  LA: 'LATAM Airlines',
  SK: 'Scandinavian Airlines (SAS)',
  TP: 'TAP Air Portugal',
};

export const carrierCodeToName = (code: string) => {
  return CARRIER_ABRV_MAP[code] || 'Unknown Carrier';
};
