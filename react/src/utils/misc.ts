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
