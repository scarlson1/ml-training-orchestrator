import AirplanemodeActiveRounded from '@mui/icons-material/AirplanemodeActiveRounded';
import type { SvgIconProps } from '@mui/material';
import { AirCanadaLogo } from '~/assets/AirCanadaLogo';
import { AirFranceLogo } from '~/assets/AirFranceLogo';
import { AlaskaLogo } from '~/assets/AlaskaLogo';
import { AllegiantLogo } from '~/assets/AllegiantLogo';
import { AmericanLogo } from '~/assets/AmericanLogo';
import { BritishAirLogo } from '~/assets/BritishAirLogo';
import { DeltaLogo } from '~/assets/DeltaLogo';
import { FrontierLogo } from '~/assets/FrontierLogo';
import { HawaiianLogo } from '~/assets/HawaiianLogo';
import { JetBlueLogo } from '~/assets/JetBlueLogo';
import { QantasLogo } from '~/assets/QantasLogo';
import { SouthwestLogo } from '~/assets/SouthwestLogo';
import { UnitedLogo } from '~/assets/UnitedLogo';
import { VirginLogo } from '~/assets/VirginLogo';

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

export const getCarrierLogo = (carrier: string, props?: SvgIconProps) => {
  switch (carrier) {
    case 'AA':
      return <AmericanLogo fontSize='small' {...props} />;
    case 'DL':
      return <DeltaLogo fontSize='small' {...props} />;
    case 'UA':
      return <UnitedLogo fontSize='small' {...props} />;
    case 'WN':
      return <SouthwestLogo fontSize='small' {...props} />;
    case 'AS':
      return <AlaskaLogo fontSize='small' {...props} />;
    case 'B6':
      return <JetBlueLogo fontSize='small' {...props} />;
    case 'F9':
      return <FrontierLogo fontSize='small' {...props} />;
    case 'HA':
      return <HawaiianLogo fontSize='small' {...props} />;
    case 'G4':
      return <AllegiantLogo fontSize='small' {...props} />;
    case 'VS':
      return <VirginLogo fontSize='small' {...props} />;
    case 'QF':
      return <QantasLogo fontSize='small' {...props} />;
    case 'AC':
      return <AirCanadaLogo fontSize='small' {...props} />;
    case 'AF':
      return <AirFranceLogo fontSize='small' {...props} />;
    case 'BA':
      return <BritishAirLogo fontSize='small' {...props} />;
    default:
      return (
        <AirplanemodeActiveRounded
          color='secondary'
          fontSize='small'
          {...props}
        />
      );
  }
};

// Derive from API ? Form error if flight is selected and carrier option doesn't match
export const CARRIERS = [
  { name: 'Aegean Airlines', code: 'A3' },
  { name: 'Aer Lingus', code: 'EI' },
  { name: 'Aeromexico', code: 'AM' },
  { name: 'Air Canada', code: 'AC' },
  { name: 'Air China', code: 'CA' },
  { name: 'Air France', code: 'AF' },
  { name: 'Air India', code: 'AI' },
  { name: 'Air New Zealand', code: 'NZ' },
  { name: 'Alaska Airlines', code: 'AS' },
  { name: 'All Nippon Airways', code: 'NH' },
  { name: 'American Airlines', code: 'AA' },
  { name: 'British Airways', code: 'BA' },
  { name: 'Cathay Pacific', code: 'CX' },
  { name: 'China Airlines', code: 'CI' },
  { name: 'Delta Air Lines', code: 'DL' },
  { name: 'Emirates', code: 'EK' },
  { name: 'Ethiopian Airlines', code: 'ET' },
  { name: 'EVA Air', code: 'BR' },
  { name: 'Finnair', code: 'AY' },
  { name: 'Garuda Indonesia', code: 'GA' },
  { name: 'Iberia', code: 'IB' },
  { name: 'Japan Airlines', code: 'JL' },
  { name: 'JetBlue Airways', code: 'B6' },
  { name: 'KLM Royal Dutch Airlines', code: 'KL' },
  { name: 'Korean Air', code: 'KE' },
  { name: 'Lufthansa', code: 'LH' },
  { name: 'Malaysia Airlines', code: 'MH' },
  { name: 'Qatar Airways', code: 'QR' },
  { name: 'Singapore Airlines', code: 'SQ' },
  { name: 'Southwest Airlines', code: 'WN' },
  { name: 'Thai Airways', code: 'TG' },
  { name: 'United Airlines', code: 'UA' },
  { name: 'Fiji Airways', code: 'FJ' },
];

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

  VS: 'Virgin Atlantic',
  QF: 'Qantas',
  AC: 'Air Canada',
  AF: 'Air France',
  BA: 'British Airways',

  // Major International Carriers
  CX: 'Cathay Pacific',
  EK: 'Emirates',
  ET: 'Ethiopian Airlines',
  IB: 'Iberia',
  JL: 'Japan Airlines',
  KL: 'KLM',
  LH: 'Lufthansa',
  QR: 'Qatar Airways',
  SQ: 'Singapore Airlines',
  TK: 'Turkish Airlines',
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

// only used for logging
export function getFlightCompositeId(origin: string, dest: string) {
  return `${origin}${dest}_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`; // → "AA123_20240503"
}

export interface Airport {
  name: string;
  code: string;
}

export const AIRPORTS: Airport[] = [
  {
    code: 'ABE',
    name: 'Lehigh Valley International Airport',
  },
  {
    code: 'ABI',
    name: 'Abilene Regional Airport',
  },
  {
    code: 'ABQ',
    name: 'Albuquerque International Sunport Airport',
  },
  {
    code: 'ABR',
    name: 'Aberdeen Regional Airport',
  },
  {
    code: 'ABY',
    name: 'Southwest Georgia Regional Airport',
  },
  {
    code: 'ACK',
    name: 'Nantucket Memorial Airport',
  },
  {
    code: 'ACT',
    name: 'Waco Regional Airport',
  },
  {
    code: 'ACV',
    name: 'California Redwood Coast-Humboldt County Airport',
  },
  {
    code: 'ACY',
    name: 'Atlantic City International Airport',
  },
  {
    code: 'ADK',
    name: 'Adak Airport',
  },
  {
    code: 'ADQ',
    name: 'Kodiak Airport',
  },
  {
    code: 'AEX',
    name: 'Alexandria International Airport',
  },
  {
    code: 'AGS',
    name: 'Augusta Regional At Bush Field',
  },
  {
    code: 'AKN',
    name: 'King Salmon Airport',
  },
  {
    code: 'ALB',
    name: 'Albany International Airport',
  },
  {
    code: 'ALO',
    name: 'Waterloo Regional Airport',
  },
  {
    code: 'AMA',
    name: 'Rick Husband Amarillo International Airport',
  },
  {
    code: 'ANC',
    name: 'Ted Stevens Anchorage International Airport',
  },
  {
    code: 'APN',
    name: 'Alpena County Regional Airport',
  },
  {
    code: 'ASE',
    name: 'Aspen-Pitkin County/Sardy Field',
  },
  {
    code: 'ATL',
    name: 'Hartsfield/Jackson Atlanta International Airport',
  },
  {
    code: 'ATW',
    name: 'Appleton International Airport',
  },
  {
    code: 'AUS',
    name: 'Austin-Bergstrom International Airport',
  },
  {
    code: 'AVL',
    name: 'Asheville Regional Airport',
  },
  {
    code: 'AVP',
    name: 'Wilkes-Barre/Scranton International Airport',
  },
  {
    code: 'AZA',
    name: 'Mesa Gateway Airport',
  },
  {
    code: 'AZO',
    name: 'Kalamazoo/Battle Creek International Airport',
  },
  {
    code: 'BDL',
    name: 'Bradley International Airport',
  },
  {
    code: 'BET',
    name: 'Bethel Airport',
  },
  {
    code: 'BFF',
    name: 'Scottsbluff/Western Nebraska Regional/Wm  B Heilig Field',
  },
  {
    code: 'BFL',
    name: 'Meadows Field',
  },
  {
    code: 'BGM',
    name: 'Greater Binghamton/Edwin A Link Field',
  },
  {
    code: 'BGR',
    name: 'Bangor International Airport',
  },
  {
    code: 'BHM',
    name: 'Birmingham-Shuttlesworth International Airport',
  },
  {
    code: 'BIH',
    name: 'Bishop Airport',
  },
  {
    code: 'BIL',
    name: 'Billings Logan International Airport',
  },
  {
    code: 'BIS',
    name: 'Bismarck Municipal Airport',
  },
  {
    code: 'BJI',
    name: 'Bemidji Regional Airport',
  },
  {
    code: 'BLI',
    name: 'Bellingham International Airport',
  },
  {
    code: 'BLV',
    name: 'Scott Afb/Midamerica St Louis Airport',
  },
  {
    code: 'BMI',
    name: 'Central Il Regional/Bloomington-Normal Airport',
  },
  {
    code: 'BNA',
    name: 'Nashville International Airport',
  },
  {
    code: 'BOI',
    name: 'Boise Air Trml/Gowen Field',
  },
  {
    code: 'BOS',
    name: 'General Edward Lawrence Logan International Airport',
  },
  {
    code: 'BPT',
    name: 'Jack Brooks Regional Airport',
  },
  {
    code: 'BQN',
    name: 'Rafael Hernandez Airport',
  },
  {
    code: 'BRD',
    name: 'Brainerd Lakes Regional Airport',
  },
  {
    code: 'BRO',
    name: 'Brownsville/South Padre Island International Airport',
  },
  {
    code: 'BRW',
    name: 'Wiley Post-Will Rogers Memorial Airport',
  },
  {
    code: 'BTM',
    name: 'Bert Mooney Airport',
  },
  {
    code: 'BTR',
    name: 'Baton Rouge Metro, Ryan Field',
  },
  {
    code: 'BTV',
    name: 'Patrick Leahy Burlington International Airport',
  },
  {
    code: 'BUF',
    name: 'Buffalo Niagara International Airport',
  },
  {
    code: 'BUR',
    name: 'Bob Hope Airport',
  },
  {
    code: 'BWI',
    name: 'Baltimore/Washington International Thurgood Marshall Airport',
  },
  {
    code: 'BZN',
    name: 'Bozeman Yellowstone International Airport',
  },
  {
    code: 'CAE',
    name: 'Columbia Metro Airport',
  },
  {
    code: 'CAK',
    name: 'Akron-Canton Regional Airport',
  },
  {
    code: 'CDC',
    name: 'Cedar City Regional Airport',
  },
  {
    code: 'CDV',
    name: 'Merle K (Mudhole) Smith Airport',
  },
  {
    code: 'CHA',
    name: 'Lovell Field',
  },
  {
    code: 'CHO',
    name: 'Charlottesville-Albemarle Airport',
  },
  {
    code: 'CHS',
    name: 'Charleston Afb/International Airport',
  },
  {
    code: 'CID',
    name: 'The Eastern Iowa Airport',
  },
  {
    code: 'CIU',
    name: 'Chippewa County International Airport',
  },
  {
    code: 'CKB',
    name: 'North Central West Virginia Airport',
  },
  {
    code: 'CLD',
    name: 'Mc Clellan-Palomar Airport',
  },
  {
    code: 'CLE',
    name: 'Cleveland-Hopkins International Airport',
  },
  {
    code: 'CLL',
    name: 'Easterwood Field',
  },
  {
    code: 'CLT',
    name: 'Charlotte/Douglas International Airport',
  },
  {
    code: 'CMH',
    name: 'John Glenn Columbus International Airport',
  },
  {
    code: 'CMI',
    name: 'University Of Illinois/Willard Airport',
  },
  {
    code: 'CMX',
    name: 'Houghton County Memorial Airport',
  },
  {
    code: 'COD',
    name: 'Yellowstone Regional Airport',
  },
  {
    code: 'COS',
    name: 'City Of Colorado Springs Municipal Airport',
  },
  {
    code: 'COU',
    name: 'Columbia Regional Airport',
  },
  {
    code: 'CPR',
    name: 'Casper/Natrona County International Airport',
  },
  {
    code: 'CRP',
    name: 'Corpus Christi International Airport',
  },
  {
    code: 'CRW',
    name: 'West Virginia International Yeager Airport',
  },
  {
    code: 'CVG',
    name: 'Cincinnati/Northern Kentucky International Airport',
  },
  {
    code: 'CWA',
    name: 'Central Wisconsin Airport',
  },
  {
    code: 'CYS',
    name: 'Cheyenne Regional/Jerry Olson Field',
  },
  {
    code: 'DAB',
    name: 'Daytona Beach International Airport',
  },
  {
    code: 'DAL',
    name: 'Dallas Love Field',
  },
  {
    code: 'DAY',
    name: 'James M Cox Dayton International Airport',
  },
  {
    code: 'DCA',
    name: 'Ronald Reagan Washington Ntl Airport',
  },
  {
    code: 'DDC',
    name: 'Dodge City Regional Airport',
  },
  {
    code: 'DEC',
    name: 'Decatur Airport',
  },
  {
    code: 'DEN',
    name: 'Denver International Airport',
  },
  {
    code: 'DFW',
    name: 'Dallas-Fort Worth International Airport',
  },
  {
    code: 'DHN',
    name: 'Dothan Regional Airport',
  },
  {
    code: 'DIK',
    name: 'Dickinson/Theodore Roosevelt Regional Airport',
  },
  {
    code: 'DLG',
    name: 'Dillingham Airport',
  },
  {
    code: 'DLH',
    name: 'Duluth International Airport',
  },
  {
    code: 'DRO',
    name: 'Durango-La Plata County Airport',
  },
  {
    code: 'DSM',
    name: 'Des Moines International Airport',
  },
  {
    code: 'DTW',
    name: 'Detroit Metro Wayne County Airport',
  },
  {
    code: 'DVL',
    name: 'Devils Lake Regional Airport',
  },
  {
    code: 'EAR',
    name: 'Kearney Regional Airport',
  },
  {
    code: 'EAU',
    name: 'Chippewa Valley Regional Airport',
  },
  {
    code: 'ECP',
    name: 'Northwest Florida Beaches International Airport',
  },
  {
    code: 'EGE',
    name: 'Eagle County Regional Airport',
  },
  {
    code: 'EKO',
    name: 'Elko Regional Airport',
  },
  {
    code: 'ELM',
    name: 'Elmira/Corning Regional Airport',
  },
  {
    code: 'ELP',
    name: 'El Paso International Airport',
  },
  {
    code: 'ESC',
    name: 'Delta County Airport',
  },
  {
    code: 'EUG',
    name: 'Mahlon Sweet Field',
  },
  {
    code: 'EVV',
    name: 'Evansville Regional Airport',
  },
  {
    code: 'EWN',
    name: 'Coastal Carolina Regional Airport',
  },
  {
    code: 'EWR',
    name: 'Newark Liberty International Airport',
  },
  {
    code: 'EYW',
    name: 'Key West International Airport',
  },
  {
    code: 'FAI',
    name: 'Fairbanks International Airport',
  },
  {
    code: 'FAR',
    name: 'Hector International Airport',
  },
  {
    code: 'FAT',
    name: 'Fresno Yosemite International Airport',
  },
  {
    code: 'FAY',
    name: 'Fayetteville Regional/Grannis Field',
  },
  {
    code: 'FCA',
    name: 'Glacier Park International Airport',
  },
  {
    code: 'FLG',
    name: 'Flagstaff Pulliam Airport',
  },
  {
    code: 'FLL',
    name: 'Fort Lauderdale/Hollywood International Airport',
  },
  {
    code: 'FMN',
    name: 'Four Corners Regional Airport',
  },
  {
    code: 'FNT',
    name: 'Bishop International Airport',
  },
  {
    code: 'FOD',
    name: 'Fort Dodge Regional Airport',
  },
  {
    code: 'FSD',
    name: 'Joe Foss Field',
  },
  {
    code: 'FSM',
    name: 'Fort Smith Regional Airport',
  },
  {
    code: 'FWA',
    name: 'Fort Wayne International Airport',
  },
  {
    code: 'GCC',
    name: 'Northeast Wyoming Regional Airport',
  },
  {
    code: 'GCK',
    name: 'Garden City Regional Airport',
  },
  {
    code: 'GEG',
    name: 'Spokane International Airport',
  },
  {
    code: 'GFK',
    name: 'Grand Forks International Airport',
  },
  {
    code: 'GGG',
    name: 'East Texas Regional Airport',
  },
  {
    code: 'GJT',
    name: 'Grand Junction Regional Airport',
  },
  {
    code: 'GNV',
    name: 'Gainesville Regional Airport',
  },
  {
    code: 'GPT',
    name: 'Gulfport-Biloxi International Airport',
  },
  {
    code: 'GRB',
    name: 'Green Bay/Austin Straubel International Airport',
  },
  {
    code: 'GRI',
    name: 'Central Nebraska Regional Airport',
  },
  {
    code: 'GRK',
    name: 'Robert Gray Army Air Field',
  },
  {
    code: 'GRR',
    name: 'Gerald R Ford International Airport',
  },
  {
    code: 'GSO',
    name: 'Piedmont Triad International Airport',
  },
  {
    code: 'GSP',
    name: 'Greenville Spartanburg International Airport',
  },
  {
    code: 'GST',
    name: 'Gustavus Airport',
  },
  {
    code: 'GTF',
    name: 'Great Falls International Airport',
  },
  {
    code: 'GTR',
    name: 'Golden Triangle Regional Airport',
  },
  {
    code: 'GUC',
    name: 'Gunnison-Crested Butte Regional Airport',
  },
  {
    code: 'GUF',
    name: 'Gulf Shores International/Jack Edwards Field',
  },
  {
    code: 'GUM',
    name: 'Guam International Airport',
  },
  {
    code: 'HDN',
    name: 'Yampa Valley Airport',
  },
  {
    code: 'HGR',
    name: 'Hagerstown Regional/Richard A Henson Field',
  },
  {
    code: 'HHH',
    name: 'Hilton Head Airport',
  },
  {
    code: 'HIB',
    name: 'Range Regional Airport',
  },
  {
    code: 'HLN',
    name: 'Helena Regional Airport',
  },
  {
    code: 'HNL',
    name: 'Daniel K Inouye International Airport',
  },
  {
    code: 'HOB',
    name: 'Lea County Regional Airport',
  },
  {
    code: 'HOU',
    name: 'William P Hobby Airport',
  },
  {
    code: 'HPN',
    name: 'Westchester County Airport',
  },
  {
    code: 'HRL',
    name: 'Valley International Airport',
  },
  {
    code: 'HSV',
    name: 'Huntsville International-Carl T Jones Field',
  },
  {
    code: 'HTS',
    name: 'Tri-State/Milton J Ferguson Field',
  },
  {
    code: 'HYA',
    name: 'Cape Cod Gateway Airport',
  },
  {
    code: 'HYS',
    name: 'Hays Regional Airport',
  },
  {
    code: 'IAD',
    name: 'Washington Dulles International Airport',
  },
  {
    code: 'IAG',
    name: 'Niagara Falls International Airport',
  },
  {
    code: 'IAH',
    name: 'George Bush Intcntl/Houston Airport',
  },
  {
    code: 'ICT',
    name: 'Wichita Dwight D Eisenhower Ntl Airport',
  },
  {
    code: 'IDA',
    name: 'Idaho Falls Regional Airport',
  },
  {
    code: 'ILM',
    name: 'Wilmington International Airport',
  },
  {
    code: 'IMT',
    name: 'Ford Airport',
  },
  {
    code: 'IND',
    name: 'Indianapolis International Airport',
  },
  {
    code: 'INL',
    name: 'Falls International/Einarson Field',
  },
  {
    code: 'ISP',
    name: 'Long Island Mac Arthur Airport',
  },
  {
    code: 'ITO',
    name: 'Hilo International Airport',
  },
  {
    code: 'JAC',
    name: 'Jackson Hole Airport',
  },
  {
    code: 'JAN',
    name: 'Jackson-Medgar Wiley Evers International Airport',
  },
  {
    code: 'JAX',
    name: 'Jacksonville International Airport',
  },
  {
    code: 'JFK',
    name: 'John F Kennedy International Airport',
  },
  {
    code: 'JLN',
    name: 'Joplin Regional Airport',
  },
  {
    code: 'JMS',
    name: 'Jamestown Regional Airport',
  },
  {
    code: 'JNU',
    name: 'Juneau International Airport',
  },
  {
    code: 'JST',
    name: 'John Murtha Johnstown/Cambria County Airport',
  },
  {
    code: 'KOA',
    name: 'Ellison Onizuka Kona International At Keahole Airport',
  },
  {
    code: 'KTN',
    name: 'Ketchikan International Airport',
  },
  {
    code: 'LAN',
    name: 'Capital Region International Airport',
  },
  {
    code: 'LAR',
    name: 'Laramie Regional Airport',
  },
  {
    code: 'LAS',
    name: 'Harry Reid International Airport',
  },
  {
    code: 'LAW',
    name: 'Lawton-Fort Sill Regional Airport',
  },
  {
    code: 'LAX',
    name: 'Los Angeles International Airport',
  },
  {
    code: 'LBB',
    name: 'Lubbock Preston Smith International Airport',
  },
  {
    code: 'LBE',
    name: 'Arnold Palmer Regional Airport',
  },
  {
    code: 'LBF',
    name: 'North Platte Regional/Lee Bird Field',
  },
  {
    code: 'LBL',
    name: 'Liberal Mid-America Regional Airport',
  },
  {
    code: 'LCH',
    name: 'Lake Charles Regional Airport',
  },
  {
    code: 'LCK',
    name: 'Rickenbacker International Airport',
  },
  {
    code: 'LEX',
    name: 'Blue Grass Airport',
  },
  {
    code: 'LFT',
    name: 'Lafayette Regional/Paul Fournet Field',
  },
  {
    code: 'LGA',
    name: 'Laguardia Airport',
  },
  {
    code: 'LGB',
    name: 'Long Beach (Daugherty Field) Airport',
  },
  {
    code: 'LIH',
    name: 'Lihue Airport',
  },
  {
    code: 'LIT',
    name: 'Bill And Hillary Clinton Ntl/Adams Field',
  },
  {
    code: 'LNK',
    name: 'Lincoln Airport',
  },
  {
    code: 'LRD',
    name: 'Laredo International Airport',
  },
  {
    code: 'LSE',
    name: 'La Crosse Regional Airport',
  },
  {
    code: 'LWS',
    name: 'Lewiston/Nez Perce County Airport',
  },
  {
    code: 'MAF',
    name: 'Midland International Air And Space Port Airport',
  },
  {
    code: 'MBS',
    name: 'Mbs International Airport',
  },
  {
    code: 'MCI',
    name: 'Kansas City International Airport',
  },
  {
    code: 'MCO',
    name: 'Orlando International Airport',
  },
  {
    code: 'MCW',
    name: 'Mason City Municipal Airport',
  },
  {
    code: 'MDT',
    name: 'Harrisburg International Airport',
  },
  {
    code: 'MDW',
    name: 'Chicago Midway International Airport',
  },
  {
    code: 'MEI',
    name: 'Key Field',
  },
  {
    code: 'MEM',
    name: 'Frederick W Smith International Airport',
  },
  {
    code: 'MFE',
    name: 'Mc Allen International Airport',
  },
  {
    code: 'MFR',
    name: 'Rogue Valley International/Medford Airport',
  },
  {
    code: 'MGM',
    name: 'Montgomery Regional (Dannelly Field) Airport',
  },
  {
    code: 'MGW',
    name: 'Morgantown Municipal/Walter L Bill Hart Field',
  },
  {
    code: 'MHK',
    name: 'Manhattan Regional Airport',
  },
  {
    code: 'MHT',
    name: 'Manchester Boston Regional Airport',
  },
  {
    code: 'MIA',
    name: 'Miami International Airport',
  },
  {
    code: 'MKE',
    name: 'General Mitchell International Airport',
  },
  {
    code: 'MLB',
    name: 'Melbourne Orlando International Airport',
  },
  {
    code: 'MLI',
    name: 'Quad Cities International Airport',
  },
  {
    code: 'MLU',
    name: 'Monroe Regional Airport',
  },
  {
    code: 'MOB',
    name: 'Mobile Regional Airport',
  },
  {
    code: 'MOT',
    name: 'Minot International Airport',
  },
  {
    code: 'MQT',
    name: 'Marquette/Sawyer Regional Airport',
  },
  {
    code: 'MRY',
    name: 'Monterey Regional Airport',
  },
  {
    code: 'MSN',
    name: 'Dane County Regional/Truax Field',
  },
  {
    code: 'MSO',
    name: 'Missoula Montana Airport',
  },
  {
    code: 'MSP',
    name: 'Minneapolis-St Paul International/Wold-Chamberlain Airport',
  },
  {
    code: 'MSY',
    name: 'Louis Armstrong New Orleans International Airport',
  },
  {
    code: 'MTJ',
    name: 'Montrose Regional Airport',
  },
  {
    code: 'MVY',
    name: "Martha's Vineyard Airport",
  },
  {
    code: 'MYR',
    name: 'Myrtle Beach International Airport',
  },
  {
    code: 'OAJ',
    name: 'Albert J Ellis Airport',
  },
  {
    code: 'OAK',
    name: 'Oakland San Francisco Bay Airport',
  },
  {
    code: 'OGG',
    name: 'Kahului Airport',
  },
  {
    code: 'OKC',
    name: 'Okc Will Rogers International Airport',
  },
  {
    code: 'OMA',
    name: 'Eppley Airfield',
  },
  {
    code: 'OME',
    name: 'Nome Airport',
  },
  {
    code: 'ONT',
    name: 'Ontario International Airport',
  },
  {
    code: 'ORD',
    name: "Chicago O'Hare International Airport",
  },
  {
    code: 'ORF',
    name: 'Norfolk International Airport',
  },
  {
    code: 'ORH',
    name: 'Worcester Regional Airport',
  },
  {
    code: 'OTH',
    name: 'Southwest Oregon Regional Airport',
  },
  {
    code: 'OTZ',
    name: 'Ralph Wien Memorial Airport',
  },
  {
    code: 'PAE',
    name: 'Seattle Paine Field International Airport',
  },
  {
    code: 'PBG',
    name: 'Plattsburgh International Airport',
  },
  {
    code: 'PBI',
    name: 'Palm Beach International Airport',
  },
  {
    code: 'PDX',
    name: 'Portland International Airport',
  },
  {
    code: 'PGD',
    name: 'Punta Gorda Airport',
  },
  {
    code: 'PHL',
    name: 'Philadelphia International Airport',
  },
  {
    code: 'PHX',
    name: 'Phoenix Sky Harbor International Airport',
  },
  {
    code: 'PIA',
    name: 'General Downing - Peoria International Airport',
  },
  {
    code: 'PIB',
    name: 'Hattiesburg/Laurel Regional Airport',
  },
  {
    code: 'PIE',
    name: 'St Pete-Clearwater International Airport',
  },
  {
    code: 'PIH',
    name: 'Pocatello Regional Airport',
  },
  {
    code: 'PIT',
    name: 'Pittsburgh International Airport',
  },
  {
    code: 'PLN',
    name: 'Pellston Regional/Emmet County Airport',
  },
  {
    code: 'PNS',
    name: 'Pensacola International Airport',
  },
  {
    code: 'PPG',
    name: 'Pago Pago International Airport',
  },
  {
    code: 'PQI',
    name: 'Presque Isle International Airport',
  },
  {
    code: 'PRC',
    name: 'Prescott Regional/Ernest A Love Field',
  },
  {
    code: 'PSC',
    name: 'Tri-Cities Airport',
  },
  {
    code: 'PSE',
    name: 'Mercedita Airport',
  },
  {
    code: 'PSG',
    name: 'Petersburg James A Johnson Airport',
  },
  {
    code: 'PSM',
    name: 'Portsmouth International At Pease Airport',
  },
  {
    code: 'PSP',
    name: 'Palm Springs International Airport',
  },
  {
    code: 'PVD',
    name: 'Rhode Island Tf Green International Airport',
  },
  {
    code: 'PVU',
    name: 'Provo Municipal Airport',
  },
  {
    code: 'PWM',
    name: 'Portland International Jetport Airport',
  },
  {
    code: 'RAP',
    name: 'Rapid City Regional Airport',
  },
  {
    code: 'RDD',
    name: 'Redding Regional Airport',
  },
  {
    code: 'RDM',
    name: 'Roberts Field',
  },
  {
    code: 'RDU',
    name: 'Raleigh-Durham International Airport',
  },
  {
    code: 'RFD',
    name: 'Chicago/Rockford International Airport',
  },
  {
    code: 'RHI',
    name: 'Rhinelander/Oneida County Airport',
  },
  {
    code: 'RIC',
    name: 'Richmond International Airport',
  },
  {
    code: 'RIW',
    name: 'Central Wyoming Regional Airport',
  },
  {
    code: 'RKS',
    name: 'Southwest Wyoming Regional Airport',
  },
  {
    code: 'RNO',
    name: 'Reno/Tahoe International Airport',
  },
  {
    code: 'ROA',
    name: 'Roanoke/Blacksburg Regional (Woodrum Field) Airport',
  },
  {
    code: 'ROC',
    name: 'Frederick Douglass/Greater Rochester International Airport',
  },
  {
    code: 'ROW',
    name: 'Roswell Air Center Airport',
  },
  {
    code: 'RST',
    name: 'Rochester International Airport',
  },
  {
    code: 'RSW',
    name: 'Southwest Florida International Airport',
  },
  {
    code: 'SAF',
    name: 'Santa Fe Regional Airport',
  },
  {
    code: 'SAN',
    name: 'San Diego International Airport',
  },
  {
    code: 'SAT',
    name: 'San Antonio International Airport',
  },
  {
    code: 'SAV',
    name: 'Savannah/Hilton Head International Airport',
  },
  {
    code: 'SBA',
    name: 'Santa Barbara Municipal Airport',
  },
  {
    code: 'SBN',
    name: 'South Bend International Airport',
  },
  {
    code: 'SBP',
    name: 'San Luis Obispo County Regional Airport',
  },
  {
    code: 'SCC',
    name: 'Deadhorse Airport',
  },
  {
    code: 'SCE',
    name: 'State College Regional Airport',
  },
  {
    code: 'SCK',
    name: 'Stockton Metro Airport',
  },
  {
    code: 'SDF',
    name: 'Louisville Muhammad Ali International Airport',
  },
  {
    code: 'SEA',
    name: 'Seattle-Tacoma International Airport',
  },
  {
    code: 'SFB',
    name: 'Orlando Sanford International Airport',
  },
  {
    code: 'SFO',
    name: 'San Francisco International Airport',
  },
  {
    code: 'SGF',
    name: 'Springfield-Branson Ntl Airport',
  },
  {
    code: 'SGU',
    name: 'St George Regional Airport',
  },
  {
    code: 'SHR',
    name: 'Sheridan County Airport',
  },
  {
    code: 'SHV',
    name: 'Shreveport Regional Airport',
  },
  {
    code: 'SIT',
    name: 'Sitka Rocky Gutierrez Airport',
  },
  {
    code: 'SJC',
    name: 'Norman Y Mineta San Jose International Airport',
  },
  {
    code: 'SJT',
    name: 'San Angelo Regional/Mathis Field',
  },
  {
    code: 'SJU',
    name: 'Luis Munoz Marin International Airport',
  },
  {
    code: 'SLC',
    name: 'Salt Lake City International Airport',
  },
  {
    code: 'SLN',
    name: 'Salina Regional Airport',
  },
  {
    code: 'SMF',
    name: 'Sacramento International Airport',
  },
  {
    code: 'SMX',
    name: 'Santa Maria Pub/Capt G Allan Hancock Field',
  },
  {
    code: 'SNA',
    name: 'John Wayne/Orange County Airport',
  },
  {
    code: 'SPI',
    name: 'Abraham Lincoln Capital Airport',
  },
  {
    code: 'SPS',
    name: 'Sheppard Afb/Wichita Falls Municipal Airport',
  },
  {
    code: 'SRQ',
    name: 'Sarasota/Bradenton International Airport',
  },
  {
    code: 'STC',
    name: 'St Cloud Regional Airport',
  },
  {
    code: 'STL',
    name: 'St Louis Lambert International Airport',
  },
  {
    code: 'STS',
    name: 'Charles M Schulz/Sonoma County Airport',
  },
  {
    code: 'STT',
    name: 'Cyril E King Airport',
  },
  {
    code: 'STX',
    name: 'Henry E Rohlsen Airport',
  },
  {
    code: 'SUN',
    name: 'Friedman Memorial Airport',
  },
  {
    code: 'SUX',
    name: 'Sioux Gateway/Brig General Bud Day Field',
  },
  {
    code: 'SWF',
    name: 'New York Stewart International Airport',
  },
  {
    code: 'SWO',
    name: 'Stillwater Regional Airport',
  },
  {
    code: 'SYR',
    name: 'Syracuse Hancock International Airport',
  },
  {
    code: 'TLH',
    name: 'Tallahassee International Airport',
  },
  {
    code: 'TOL',
    name: 'Eugene F Kranz Toledo Express Airport',
  },
  {
    code: 'TPA',
    name: 'Tampa International Airport',
  },
  {
    code: 'TRI',
    name: 'Tri-Cities Airport',
  },
  {
    code: 'TTN',
    name: 'Trenton Mercer Airport',
  },
  {
    code: 'TUL',
    name: 'Tulsa International Airport',
  },
  {
    code: 'TUS',
    name: 'Tucson International Airport',
  },
  {
    code: 'TVC',
    name: 'Cherry Capital Airport',
  },
  {
    code: 'TWF',
    name: 'Joslin Field/Magic Valley Regional Airport',
  },
  {
    code: 'TXK',
    name: 'Texarkana Regional-Webb Field',
  },
  {
    code: 'TYR',
    name: 'Tyler Pounds Regional Airport',
  },
  {
    code: 'TYS',
    name: 'Mc Ghee Tyson Airport',
  },
  {
    code: 'USA',
    name: 'Concord-Padgett Regional Airport',
  },
  {
    code: 'VCT',
    name: 'Victoria Regional Airport',
  },
  {
    code: 'VPS',
    name: 'Eglin Afb/Destin-Ft Walton Beach Airport',
  },
  {
    code: 'WRG',
    name: 'Wrangell Airport',
  },
  {
    code: 'WYS',
    name: 'Yellowstone Airport',
  },
  {
    code: 'XNA',
    name: 'Northwest Arkansas Ntl Airport',
  },
  {
    code: 'XWA',
    name: 'Williston Basin International Airport',
  },
  {
    code: 'YAK',
    name: 'Yakutat Airport',
  },
  {
    code: 'YUM',
    name: 'Yuma Mcas/Yuma International Airport',
  },
];
